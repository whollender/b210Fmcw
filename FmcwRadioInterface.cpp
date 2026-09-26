//
// Radio interface for B210 based FMCW radar
// Runs individual frequency ramp sweeps w/
// corresponding recv data that is then piped to
// a ZMQ PUSH socket
//
//

#include "wavetable.hpp"
#include <uhd/exception.hpp>
#include <uhd/types/tune_request.hpp>
#include <uhd/usrp/multi_usrp.hpp>
#include <uhd/utils/math.hpp>
#include <uhd/utils/safe_main.hpp>
#include <uhd/utils/static.hpp>
#include <uhd/utils/thread.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <boost/program_options.hpp>
#include <chrono>
#include <cmath>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <semaphore>
#include <thread>
#include <zmq.hpp>

using namespace std::chrono_literals;
namespace po = boost::program_options;

// Initialize the signaling semaphores such that each will wait for a 'post' call
boost::interprocess::interprocess_semaphore rx_ready(0),tx_done(0);

// Copied from uhd/host/lib/include/uhdlib/usrp/cores/dsp_core_utils.hpp
// template word width forced to 32 bits
void get_freq_and_freq_word(const double requested_freq,
    const double tick_rate,
    double& actual_freq,
    int32_t& freq_word)
{
    constexpr int32_t MAX_FREQ_WORD = std::numeric_limits<int32_t>::max();
    constexpr int32_t MIN_FREQ_WORD = std::numeric_limits<int32_t>::min();
    constexpr double scale_factor   = static_cast<double>(uint64_t(1) << 32);
    // Frequency normalized by sampling rate, and wrapped to [-0.5, 0.5).
    const double freq_norm_scaled =
        uhd::math::wrap_frequency(requested_freq / tick_rate, 1.0) * scale_factor;

    // confirm that the target frequency is within range of the CORDIC
    UHD_ASSERT_THROW(std::abs(freq_norm_scaled) - 1 <= MAX_FREQ_WORD);

    /* Now calculate the frequency word. It is possible for this calculation
     * to cause an overflow. As the requested DSP frequency approaches the
     * master clock rate, that ratio multiplied by the scaling factor (2^32)
     * will generally overflow within the last few kHz of tunable range.
     * Thus, we check to see if the operation will overflow before doing it,
     * and if it will, we set it to the integer min or max of this system.
     */
    if (freq_norm_scaled >= MAX_FREQ_WORD) {
        /* Operation would have caused a positive overflow of int32. */
        freq_word = MAX_FREQ_WORD;

    } else if (freq_norm_scaled <= MIN_FREQ_WORD) {
        /* Operation would have caused a negative overflow of int32. */
        freq_word = MIN_FREQ_WORD;

    } else {
        /* The operation is safe. Perform normally. */
        freq_word = int32_t(std::lround(freq_norm_scaled));
    }

    actual_freq = (double(freq_word) / scale_factor) * tick_rate;
}


/***********************************************************************
 * Signal handlers
 **********************************************************************/
static bool stop_signal_called = false;
void sig_int_handler(int)
{
    stop_signal_called = true;
}

/***********************************************************************
 * Utilities
 **********************************************************************/
//! Change to filename, e.g. from usrp_samples.dat to usrp_samples.00.dat,
//  but only if multiple names are to be generated.
std::string generate_out_filename(
    const std::string& base_fn, size_t n_names, size_t this_name)
{
    if (n_names == 1) {
        return base_fn;
    }

    std::filesystem::path base_fn_fp(base_fn);
    base_fn_fp.replace_extension(std::filesystem::path(
        str(boost::format("%02d%s") % this_name % base_fn_fp.extension().string())));
    return base_fn_fp.string();
}


/***********************************************************************
 * transmit_worker function
 * A function to be used in a thread for transmitting
 **********************************************************************/
void transmit_worker(int numSampPerBurst,
    uhd::tx_streamer::sptr tx_streamer,
    std::chrono::milliseconds perBurstWait_ms)
{
    std::vector<std::complex<float>> buff(numSampPerBurst);
    // fill the buffer constant samples
    for (size_t n = 0; n < buff.size(); n++) {
        buff[n] = 0.5;
    }

    // Each 'send' is a single burst to be sent immediately
    uhd::tx_metadata_t metadata;
    metadata.start_of_burst = true;
    metadata.end_of_burst = true;
    metadata.has_time_spec = false;

    // send data until the signal handler gets called
    while (not stop_signal_called) {

        // Wait for semaphore
        rx_ready.wait();
        if(perBurstWait_ms.count() > 0)
        {
            std::this_thread::sleep_for(perBurstWait_ms);
        }

        // send the entire contents of the buffer
        tx_streamer->send({&buff.front()}, buff.size(), metadata);
        tx_done.post();
    }

    // Make sure we allow the rx thread to finish at this point
    tx_done.post();
}


/***********************************************************************
 * recv_to_file function
 **********************************************************************/
void recv_to_file(uhd::usrp::multi_usrp::sptr usrp,
    int socketPort,
    int num_requested_samples)
{
    int num_total_samps = 0;

    // Bind zmq
    zmq::context_t context (1);
    zmq::socket_t socket (context, zmq::socket_type::push);
    //std::string bind_string = "tcp://*:" << socketPort;
    std::string bind_string = "tcp://*:5555";
    std::cout << "Binding to " << bind_string;
    socket.bind (bind_string);

    // create a receive streamer
    // Use default otw format, but get data as float
    uhd::stream_args_t stream_args("fc32", "sc16");
    stream_args.channels             = {0};
    uhd::rx_streamer::sptr rx_stream = usrp->get_rx_stream(stream_args);

    // Prepare buffers for received samples and metadata
    uhd::rx_metadata_t md;
    std::vector<std::complex<float>> buff(num_requested_samples);
    // create a vector of pointers to point to each of the channel buffers

    bool overflow_message = true;
    // We increase the first timeout to cover for the delay between now + the
    // command time, plus 500ms of buffer. In the loop, we will then reduce the
    // timeout for subsequent receives.
    double timeout = 0.1f;

    while (not stop_signal_called){
        // setup streaming
        uhd::stream_cmd_t stream_cmd(uhd::stream_cmd_t::STREAM_MODE_NUM_SAMPS_AND_DONE);
        stream_cmd.num_samps  = num_requested_samples;
        stream_cmd.stream_now = false;
        // Need to set a time-spec that is AFTER the tx trigger time
        stream_cmd.time_spec  = usrp->get_time_now() + uhd::time_spec_t(1.0f);
        stream_cmd.trigger = uhd::stream_cmd_t::trigger_t::TX_RUNNING;
        
        
        rx_stream->issue_stream_cmd(stream_cmd);
        
        // Tell the tx thread that it can go
        rx_ready.post();

        // Now wait for the tx thread to finish 'send'ing
        tx_done.wait();

        size_t num_rx_samps = rx_stream->recv({&buff.front()}, num_requested_samples, md, timeout);

        // We're doing small bursts, so we should always get what we want
        // Rolling Stones not included
        if(num_rx_samps != num_requested_samples)
        {
            std::cout << "recv did not return requested samples!" << std::endl;
            break;
        }

        if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_TIMEOUT) {
            std::cout << "Timeout while streaming" << std::endl;
            break;
        }
        if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_OVERFLOW) {
            if (overflow_message) {
                overflow_message = false;
                std::cerr
                    << boost::format(
                           "Got an overflow indication. Please consider the following:\n"
                           "  Your write medium must sustain a rate of %fMB/s.\n"
                           "  Dropped samples will not be written to the file.\n"
                           "  Please modify this example for your purposes.\n"
                           "  This message will not appear again.\n")
                           % (usrp->get_rx_rate() * sizeof(std::complex<float>) / 1e6);
            }
            continue;
        }
        if (md.error_code != uhd::rx_metadata_t::ERROR_CODE_NONE) {
            throw std::runtime_error("Receiver error " + md.strerror());
        }

        // ZMQ send
        socket.send(zmq::buffer(buff,num_rx_samps*sizeof(std::complex<float>)),zmq::send_flags::none);
    }

    // Make sure to post as we break out of the loop so that we don't get stuck
    rx_ready.post();
}


/***********************************************************************
 * Main function
 **********************************************************************/
int UHD_SAFE_MAIN(int argc, char* argv[])
{
    const std::string program_doc = "Read the code";
    // transmit variables to be set by po
    std::string tx_channels = "0";
    std::string ref = "internal";
    std::string otw = "sc16";
    std::string wave_type = "CONST";
    double wave_freq = 0.0;
    double tx_rate, tx_freq, tx_gain, tx_bw;
    float ampl = 0.5;

    // receive variables to be set by po
    std::string rx_channels = "0";
    size_t total_num_samps, spb;
    int socketPort;
    double rx_rate, rx_gain, rx_bw;
    double settling;

    // tx sweep params
    double sweep_start_freq, sweep_stop_freq, sweep_rate;

    std::string fpgaImage;
    double masterClockRate_Hz;

    int preTxDelay_ms;

    // setup the program options
    po::options_description desc("Allowed options");
    // clang-format off
    desc.add_options()
        ("help,h", "Show this help message and exit.")
        ("fpga-image-path", po::value<std::string>(&fpgaImage)->default_value("/home/will/dev/b210Fmcw/txRxTrigTest.bit"), "FPGA Image Path")
        ("master-clock-rate", po::value<double>(&masterClockRate_Hz)->default_value(51.2e6), "Master clock rate")
        ("socket", po::value<int>(&socketPort)->default_value(5555), "Bind port for ZMQ Push socket")
        ("tx-rate", po::value<double>(&tx_rate)->default_value(200e3), "TX sample rate in samples/second. Note that "
            "each USRP device only supports a set of discrete sample rates, which depend on the hardware model and "
            "configuration. If you request a rate that is not supported, the USRP device will automatically select and "
            "use the closest available rate.")
        ("rx-rate", po::value<double>(&rx_rate)->default_value(200e3), "RX sample rate in samples/second.")
        ("tx-freq", po::value<double>(&tx_freq)->default_value(5.8e9), "TX RF center frequency in Hz.")
        ("tx-gain", po::value<double>(&tx_gain), "TX gain for the RF chain in dB.")
        ("rx-gain", po::value<double>(&rx_gain), "RX gain for the RF chain in dB.")
        ("tx-triangle-sweep", "triangular sweep")
        ("tx-sweep-start", po::value<double>(&sweep_start_freq)->default_value(-19.2e6), "Sweep start frequency in Hz.  Must be within tx rate")
        ("tx-sweep-stop", po::value<double>(&sweep_stop_freq)->default_value(19.2e6), "Sweep stop frequency in Hz.  Must be within tx rate")
        ("tx-sweep-rate", po::value<double>(&sweep_rate)->default_value(39.0625e9), "Sweep rate in Hz/s")
        ("tx-burst-delay", po::value<int>(&preTxDelay_ms)->default_value(100), "tx delay per burst in milliseconds")
    ;
    // clang-format on
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    if (vm.count("help")) {
        std::cout << program_doc << std::endl;
        std::cout << desc << std::endl;
        return ~0;
    }
    po::notify(vm); // only called if --help was not requested

    std::string tx_args = boost::str(boost::format("fpga=%s,enable_user_regs,master_clock_rate=%g") % fpgaImage % masterClockRate_Hz);

    // create a usrp device
    std::cout << std::endl;
    std::cout << boost::format("Creating the transmit usrp device with: %s...") % tx_args
              << std::endl;
    uhd::usrp::multi_usrp::sptr tx_usrp = uhd::usrp::multi_usrp::make(tx_args);
    std::cout << std::endl;
    uhd::usrp::multi_usrp::sptr rx_usrp = uhd::usrp::multi_usrp::make("");

    // detect which channels to use
    std::vector<std::string> tx_channel_strings;
    std::vector<size_t> tx_channel_nums;
    boost::split(tx_channel_strings, tx_channels, boost::is_any_of("\"',"));
    for (size_t ch = 0; ch < tx_channel_strings.size(); ch++) {
        size_t chan = std::stoi(tx_channel_strings[ch]);
        if (chan >= tx_usrp->get_tx_num_channels()) {
            throw std::runtime_error("Invalid TX channel(s) specified.");
        } else
            tx_channel_nums.push_back(std::stoi(tx_channel_strings[ch]));
    }
    std::vector<std::string> rx_channel_strings;
    std::vector<size_t> rx_channel_nums;
    boost::split(rx_channel_strings, rx_channels, boost::is_any_of("\"',"));
    for (size_t ch = 0; ch < rx_channel_strings.size(); ch++) {
        size_t chan = std::stoi(rx_channel_strings[ch]);
        if (chan >= rx_usrp->get_rx_num_channels()) {
            throw std::runtime_error("Invalid RX channel(s) specified.");
        } else
            rx_channel_nums.push_back(std::stoi(rx_channel_strings[ch]));
    }

    std::cout << "Using TX Device: " << tx_usrp->get_pp_string() << std::endl;
    std::cout << "Using RX Device: " << rx_usrp->get_pp_string() << std::endl;

    std::cout << boost::format("Setting TX Rate: %f Msps...") % (tx_rate / 1e6)
              << std::endl;
    tx_usrp->set_tx_rate(tx_rate);
    std::cout << boost::format("Actual TX Rate: %f Msps...")
                     % (tx_usrp->get_tx_rate() / 1e6)
              << std::endl
              << std::endl;

    // set the receive sample rate
    std::cout << boost::format("Setting RX Rate: %f Msps...") % (rx_rate / 1e6)
              << std::endl;
    rx_usrp->set_rx_rate(rx_rate);
    std::cout << boost::format("Actual RX Rate: %f Msps...")
                     % (rx_usrp->get_rx_rate() / 1e6)
              << std::endl
              << std::endl;


    // Setup the tx freq sweep params
    auto userRegIface = tx_usrp->get_user_settings_iface();
    if(!userRegIface)
    {
        std::cerr << "User settings interface return nullptr!" << std::endl;
        return 1;
    }

    // ADDR 0 is settings word
    // Only 2 bits in settings word for now.  bit 0 is enable
    // bit 1 is triangle
    // bit 2 is tx/rx mixing
    // Enable bits 0 and 2 (= 0x5) for sweep and mixing
    uint32_t sweepSettingWord = 5u;
    sweepSettingWord += vm.count("tx-triangle-sweep") ? 2u : 0u;
    userRegIface->poke32(0, sweepSettingWord);

    // ADDR 4 is sweep start freq
    // ADDR 8 is sweep stop freq
    // Need master clock rate
    double tick_rate = tx_usrp->get_master_clock_rate();
    double actStartFreq, actStopFreq;
    int32_t actStartWord, actStopWord;
    get_freq_and_freq_word(sweep_start_freq, tick_rate, actStartFreq, actStartWord);
    get_freq_and_freq_word(sweep_stop_freq, tick_rate, actStopFreq, actStopWord);
    std::cout << boost::format("Setting sweep start freq: %f MHz ...") % (actStartFreq/1e6)
                << std::endl;
    std::cout << boost::format("Setting sweep stop freq: %f MHz ...") % (actStopFreq/1e6)
                << std::endl;

    std::cout << boost::format("freq start word: 0x%08x, stop word: 0x%08x") % actStartWord % actStopWord
                << std::endl;

    userRegIface->poke32(0x4,actStartWord);
    userRegIface->poke32(0x8,actStopWord);

    // ADDR C is sweep rate
    // Need to convert from Hz/s to freq word per tick
    // Convert from 1/s to 1/Sa is factor of 1/tick_rate
    // Convert from Hz to freq word is factor of 1/tick_rate
    double maxUInt32 = std::numeric_limits<uint32_t>::max();
    int32_t sweepWord = static_cast<int32_t>(std::floor(sweep_rate*maxUInt32/(tick_rate*tick_rate)));
    double actSweepPerSecond = sweepWord*tick_rate*tick_rate/maxUInt32;
    std::cout << boost::format("Actual sweep rate: %f MHz/s") % (actSweepPerSecond/1e6)
                << std::endl;
    std::cout << boost::format("Sweep word: 0x%08x") % sweepWord
                << std::endl;

    userRegIface->poke32(0xC,sweepWord);

    // Readback for funsies
    uint64_t readback1 = userRegIface->peek64(0x0);
    uint64_t readback2 = userRegIface->peek64(0x8);

    std::cout << boost::format("user reg readback: 0x0: 0x%016X, 0x8: 0x%016X") % readback1 % readback2
                << std::endl;

    // Need to calculate the actual number of samples we need to read at the output rate
    double actSweepTime = (actStopFreq - actStartFreq) / actSweepPerSecond;
    int32_t sweepSamplesAtMClk = std::round(std::floor(actSweepTime*masterClockRate_Hz));
    std::cout << boost::format("Actual sweep time %f seconds, %d samples at mclk") % actSweepTime % sweepSamplesAtMClk
              << std::endl;

    float decRate = masterClockRate_Hz / rx_rate;
    int32_t rxSamples = std::round(std::floor(sweepSamplesAtMClk/decRate));

    std::cout << boost::format("Number of rec samples required at output rate: %d") % rxSamples << std::endl;

    // And the number of samples that we need to 'transmit'
    float intRate = masterClockRate_Hz / tx_rate;
    int32_t txSamples = std::round(std::floor(sweepSamplesAtMClk/intRate));
    std::cout << boost::format("Number of tx samples required at input rate: %d") % txSamples << std::endl;

    for (size_t ch = 0; ch < tx_channel_nums.size(); ch++) {
        size_t channel = tx_channel_nums[ch];
        if (tx_channel_nums.size() > 1) {
            std::cout << "Configuring TX Channel " << channel << std::endl;
        }
        std::cout << boost::format("Setting TX Freq: %f MHz...") % (tx_freq / 1e6)
                  << std::endl;
        uhd::tune_request_t tx_tune_request(tx_freq);
        tx_usrp->set_tx_freq(tx_tune_request, channel);
        std::cout << boost::format("Actual TX Freq: %f MHz...")
                         % (tx_usrp->get_tx_freq(channel) / 1e6)
                  << std::endl
                  << std::endl;

        // set the rf gain
        if (vm.count("tx-gain")) {
            std::cout << boost::format("Setting TX Gain: %f dB...") % tx_gain
                      << std::endl;
            tx_usrp->set_tx_gain(tx_gain, channel);
            std::cout << boost::format("Actual TX Gain: %f dB...")
                             % tx_usrp->get_tx_gain(channel)
                      << std::endl
                      << std::endl;
        }

        // set the analog frontend filter bandwidth
        if (vm.count("tx-bw")) {
            std::cout << boost::format("Setting TX Bandwidth: %f MHz...") % tx_bw
                      << std::endl;
            tx_usrp->set_tx_bandwidth(tx_bw, channel);
            std::cout << boost::format("Actual TX Bandwidth: %f MHz...")
                             % tx_usrp->get_tx_bandwidth(channel)
                      << std::endl
                      << std::endl;
        }
    }

    for (size_t ch = 0; ch < rx_channel_nums.size(); ch++) {
        size_t channel = rx_channel_nums[ch];
        if (rx_channel_nums.size() > 1) {
            std::cout << "Configuring RX Channel " << channel << std::endl;
        }

        std::cout << boost::format("Setting RX Freq: %f MHz...") % (tx_freq / 1e6)
                  << std::endl;
        uhd::tune_request_t rx_tune_request(tx_freq);
        rx_usrp->set_rx_freq(rx_tune_request, channel);
        std::cout << boost::format("Actual RX Freq: %f MHz...")
                         % (rx_usrp->get_rx_freq(channel) / 1e6)
                  << std::endl
                  << std::endl;

        // set the receive rf gain
        if (vm.count("rx-gain")) {
            std::cout << boost::format("Setting RX Gain: %f dB...") % rx_gain
                      << std::endl;
            rx_usrp->set_rx_gain(rx_gain, channel);
            std::cout << boost::format("Actual RX Gain: %f dB...")
                             % rx_usrp->get_rx_gain(channel)
                      << std::endl
                      << std::endl;
        }

        // set the receive analog frontend filter bandwidth
        if (vm.count("rx-bw")) {
            std::cout << boost::format("Setting RX Bandwidth: %f MHz...") % (rx_bw / 1e6)
                      << std::endl;
            rx_usrp->set_rx_bandwidth(rx_bw, channel);
            std::cout << boost::format("Actual RX Bandwidth: %f MHz...")
                             % (rx_usrp->get_rx_bandwidth(channel) / 1e6)
                      << std::endl
                      << std::endl;
        }
    }

    // Align times in the RX USRP (the TX USRP does not require time-syncing)
    if (rx_usrp->get_num_mboards() > 1) {
        rx_usrp->set_time_unknown_pps(uhd::time_spec_t(0.0));
    }

    // create a transmit streamer
    // linearly map channels (index0 = channel0, index1 = channel1, ...)
    uhd::stream_args_t stream_args("fc32", otw);
    stream_args.channels             = tx_channel_nums;
    uhd::tx_streamer::sptr tx_stream = tx_usrp->get_tx_stream(stream_args);

    // Check Ref and LO Lock detect
    std::vector<std::string> tx_sensor_names, rx_sensor_names;
    for (size_t ch = 0; ch < tx_channel_nums.size(); ch++) {
        size_t channel  = tx_channel_nums[ch];
        tx_sensor_names = tx_usrp->get_tx_sensor_names(channel);
        if (std::find(tx_sensor_names.begin(), tx_sensor_names.end(), "lo_locked")
            != tx_sensor_names.end()) {
            uhd::sensor_value_t lo_locked = tx_usrp->get_tx_sensor("lo_locked", channel);
            std::cout << boost::format("Checking TX Channel %d: %s ...") % channel
                             % lo_locked.to_pp_string()
                      << std::endl;
            if (!lo_locked.to_bool()) {
                throw uhd::runtime_error(
                    "ERROR: LO is not locked for TX channel " + std::to_string(channel)
                    + ". Ensure frequency is supported, check cabling for external "
                      "reference clock if applicable, try increasing settling time, "
                      "verify that TX/RX frequencies match for shared LO "
                      "daughterboards.");
            }
        }
    }
    for (size_t ch = 0; ch < rx_channel_nums.size(); ch++) {
        size_t channel  = rx_channel_nums[ch];
        rx_sensor_names = rx_usrp->get_rx_sensor_names(channel);
        if (std::find(rx_sensor_names.begin(), rx_sensor_names.end(), "lo_locked")
            != rx_sensor_names.end()) {
            uhd::sensor_value_t lo_locked = rx_usrp->get_rx_sensor("lo_locked", channel);
            std::cout << boost::format("Checking RX Channel %d: %s ...") % channel
                             % lo_locked.to_pp_string()
                      << std::endl;
            if (!lo_locked.to_bool()) {
                throw uhd::runtime_error(
                    "ERROR: LO is not locked for RX channel " + std::to_string(channel)
                    + ". Ensure frequency is supported, check cabling for external "
                      "reference clock if applicable, try increasing settling time, "
                      "verify that TX/RX frequencies match for shared LO "
                      "daughterboards.");
            }
        }
    }

    std::signal(SIGINT, &sig_int_handler);
    std::cout << "Press Ctrl + C to stop streaming..." << std::endl;

    // reset usrp time to prepare for transmit/receive
    std::cout << boost::format("Setting device timestamp to 0...") << std::endl;
    tx_usrp->set_time_now(uhd::time_spec_t(0.0));

    // start transmit worker thread
    std::thread transmit_thread([&]() {
        transmit_worker(txSamples, tx_stream, std::chrono::milliseconds(preTxDelay_ms));
    });

    bool useTxTrig = true;

    // recv to file
    recv_to_file(
        rx_usrp, socketPort, rxSamples);

    // clean up transmit worker
    stop_signal_called = true;
    transmit_thread.join();

    // finished
    std::cout << std::endl << "Done!" << std::endl << std::endl;
    return EXIT_SUCCESS;
}
