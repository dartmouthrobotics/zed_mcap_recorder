#include <rclcpp/rclcpp.hpp>
#include <rclcpp_components/register_node_macro.hpp>
#include <sensor_msgs/msg/compressed_image.hpp>
#include <sensor_msgs/msg/imu.hpp>
#include <rosbag2_cpp/writer.hpp>
#include <rosbag2_storage/storage_options.hpp>

#include <mutex>
#include <atomic>
#include <thread>
#include <queue>
#include <condition_variable>
#include <iomanip>
#include <variant>
#include <cstring>

#include <rcutils/error_handling.h>

#if !defined(NDEBUG)
#define ZED_MCAP_DEBUG_STATS 1
#else
#define ZED_MCAP_DEBUG_STATS 0
#endif

namespace zed_mcap_recorder {

using ImageMsg = sensor_msgs::msg::CompressedImage;
using ImuMsg = sensor_msgs::msg::Imu;
using MessageVariant = std::variant<
    std::unique_ptr<ImageMsg>,
    std::unique_ptr<ImuMsg>>;

struct PendingMessage {
  std::string topic_name;
  MessageVariant msg;
  int64_t timestamp;
};

class ZedMcapRecorder : public rclcpp::Node {
public:
  explicit ZedMcapRecorder(const rclcpp::NodeOptions& options)
    : Node("zed_mcap_recorder", options)
  {
    declare_parameter("bag_path", "");
    declare_parameter("recording", false);
    declare_parameter("queue_depth_image", 60);
    declare_parameter("queue_depth_imu", 200);
    declare_parameter("topics", std::vector<std::string>{});

    bag_path_ = get_parameter("bag_path").as_string();
    int img_depth = get_parameter("queue_depth_image").as_int();
    int imu_depth = get_parameter("queue_depth_imu").as_int();

    // Separate callback groups
    img_cb_group_ = create_callback_group(
        rclcpp::CallbackGroupType::MutuallyExclusive);
    imu_cb_group_ = create_callback_group(
        rclcpp::CallbackGroupType::MutuallyExclusive);

  #if ZED_MCAP_DEBUG_STATS
    last_report_time_ = std::chrono::steady_clock::now();
  #endif

    writer_thread_ = std::thread(&ZedMcapRecorder::writer_loop, this);

    if (get_parameter("recording").as_bool()) {
      start_recording();
    }

    param_cb_ = add_on_set_parameters_callback(
      [this](const std::vector<rclcpp::Parameter>& params) {
        rcl_interfaces::msg::SetParametersResult result;
        result.successful = true;
        for (const auto& p : params) {
          if (p.get_name() == "recording") {
            if (p.as_bool()) start_recording();
            else stop_recording();
          }
        }
        return result;
      });

    auto img_qos = rclcpp::QoS(rclcpp::KeepLast(img_depth))
        .reliable()
        .durability_volatile();
    auto imu_qos = rclcpp::QoS(rclcpp::KeepLast(imu_depth))
        .reliable()
        .durability_volatile();

    auto topics = get_parameter("topics").as_string_array();
    for (const auto& topic : topics) {
      if (topic.find("imu") != std::string::npos) {
        rclcpp::SubscriptionOptions imu_opts;
        imu_opts.callback_group = imu_cb_group_;
        imu_subs_.push_back(
          create_subscription<ImuMsg>(
            topic, imu_qos,
            [this, topic](ImuMsg::UniquePtr msg) {
              if (!recording_) return;
              auto pending = std::make_unique<PendingMessage>();
              pending->topic_name = topic;
              pending->timestamp = now().nanoseconds();
              pending->msg = std::move(msg);
              {
                std::lock_guard<std::mutex> lock(queue_mutex_);
                queue_.push(std::move(pending));
              }
              queue_cv_.notify_one();
            },
            imu_opts));
      } else if (topic.find("compressed") != std::string::npos) {
        rclcpp::SubscriptionOptions img_opts;
        img_opts.callback_group = img_cb_group_;
        img_subs_.push_back(
          create_subscription<ImageMsg>(
            topic, img_qos,
            [this, topic](ImageMsg::UniquePtr msg) {
              if (!recording_) return;
              auto pending = std::make_unique<PendingMessage>();
              pending->topic_name = topic;
              pending->timestamp = now().nanoseconds();
              pending->msg = std::move(msg);
              {
                std::lock_guard<std::mutex> lock(queue_mutex_);
                queue_.push(std::move(pending));
              }
              queue_cv_.notify_one();

#if ZED_MCAP_DEBUG_STATS
              enqueued_count_++;

              auto now_steady = std::chrono::steady_clock::now();
              if (now_steady - last_report_time_ > std::chrono::seconds(5)) {
                last_report_time_ = now_steady;
                RCLCPP_DEBUG(get_logger(),
                  "Queue: %zu | Img enqueued: %lu | Written: %lu",
                  queue_.size(), enqueued_count_.load(), msg_count_.load());
              }
#endif
            },
            img_opts));
      }
    }

    RCLCPP_INFO(get_logger(), "ZED MCAP Recorder initialized (async, separate cb groups)");
  }

  ~ZedMcapRecorder() {
    shutdown_ = true;
    queue_cv_.notify_one();
    if (writer_thread_.joinable()) writer_thread_.join();
    stop_recording();
  }

private:
  // Dedicated writer thread: serialize + write to disk
  void writer_loop() {
    // Per-type serializers (created once, reused)
    rclcpp::Serialization<ImageMsg> img_serializer;
    rclcpp::Serialization<ImuMsg> imu_serializer;

    while (!shutdown_) {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      queue_cv_.wait(lock, [this] {
        return !queue_.empty() || shutdown_;
      });

      std::queue<std::unique_ptr<PendingMessage>> local_queue;
      std::swap(local_queue, queue_);
      lock.unlock();

      std::lock_guard<std::mutex> writer_lock(writer_mutex_);
      while (!local_queue.empty()) {
        auto& pending = local_queue.front();

        if (writer_ && recording_) {
          rclcpp::SerializedMessage serialized;

          // Serialize on THIS thread, not the callback thread
          std::visit([&](auto& msg_ptr) {
            using T = std::decay_t<decltype(*msg_ptr)>;
            if constexpr (std::is_same_v<T, ImageMsg>) {
              img_serializer.serialize_message(msg_ptr.get(), &serialized);
            } else if constexpr (std::is_same_v<T, ImuMsg>) {
              imu_serializer.serialize_message(msg_ptr.get(), &serialized);
            }
          }, pending->msg);

          auto bag_msg = std::make_shared<rosbag2_storage::SerializedBagMessage>();
          bag_msg->topic_name = pending->topic_name;
          bag_msg->time_stamp = pending->timestamp;

          auto& rcl_msg = serialized.get_rcl_serialized_message();
          bag_msg->serialized_data = std::shared_ptr<rcutils_uint8_array_t>(
            new rcutils_uint8_array_t,
            [](rcutils_uint8_array_t * data) {
              if (data != nullptr) {
                if (data->buffer != nullptr) {
                  auto fini_ret = rcutils_uint8_array_fini(data);
                  if (fini_ret != RCUTILS_RET_OK) {
                    rcutils_reset_error();
                  }
                }
                delete data;
              }
            });
          *bag_msg->serialized_data = rcutils_get_zero_initialized_uint8_array();
          auto ret = rcutils_uint8_array_init(
            bag_msg->serialized_data.get(),
            rcl_msg.buffer_length,
            &rcl_msg.allocator);
          if (ret != RCUTILS_RET_OK) {
            RCLCPP_ERROR(
              get_logger(),
              "Failed to allocate serialized buffer for topic '%s': %s",
              pending->topic_name.c_str(),
              rcutils_get_error_string().str);
            rcutils_reset_error();
            local_queue.pop();
            continue;
          }
          memcpy(bag_msg->serialized_data->buffer,
                 rcl_msg.buffer, rcl_msg.buffer_length);
          bag_msg->serialized_data->buffer_length = rcl_msg.buffer_length;

          writer_->write(bag_msg);
#if ZED_MCAP_DEBUG_STATS
          msg_count_++;
#endif
        }
        local_queue.pop();
      }
    }
  }

  void start_recording() {
    std::lock_guard<std::mutex> lock(writer_mutex_);
    if (recording_) return;

    auto now_time = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now_time);
    std::stringstream ss;
    ss << bag_path_ << std::put_time(std::localtime(&time_t), "%Y-%m-%d-%H-%M-%S");

    rosbag2_storage::StorageOptions storage_opts;
    storage_opts.uri = ss.str();
    storage_opts.storage_id = "mcap";
    storage_opts.max_cache_size = 4ULL * 1024 * 1024 * 1024;

    writer_ = std::make_unique<rosbag2_cpp::Writer>();
    writer_->open(storage_opts);

    auto topics = get_parameter("topics").as_string_array();
    for (const auto& topic : topics) {
      std::string type;
      if (topic.find("imu") != std::string::npos)
        type = "sensor_msgs/msg/Imu";
      else if (topic.find("compressed") != std::string::npos)
        type = "sensor_msgs/msg/CompressedImage";
      if (!type.empty()) {
        rosbag2_storage::TopicMetadata meta;
        meta.name = topic;
        meta.type = type;
        meta.serialization_format = "cdr";
        writer_->create_topic(meta);
      }
    }

    recording_ = true;
#if ZED_MCAP_DEBUG_STATS
    msg_count_ = 0;
    enqueued_count_ = 0;
#endif
    RCLCPP_INFO(get_logger(), "Recording started: %s", ss.str().c_str());
  }

  void stop_recording() {
    std::lock_guard<std::mutex> lock(writer_mutex_);
    if (!recording_) return;
    recording_ = false;
    writer_.reset();
  #if ZED_MCAP_DEBUG_STATS
    RCLCPP_INFO(get_logger(), "Recording stopped. Total messages: %lu",
                msg_count_.load());
  #else
    RCLCPP_INFO(get_logger(), "Recording stopped.");
  #endif
  }

  std::string bag_path_;
  std::unique_ptr<rosbag2_cpp::Writer> writer_;
  std::mutex writer_mutex_;
  std::atomic<bool> recording_{false};
  std::atomic<bool> shutdown_{false};
  std::atomic<uint64_t> msg_count_{0};

  std::queue<std::unique_ptr<PendingMessage>> queue_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::thread writer_thread_;

  std::vector<rclcpp::Subscription<ImageMsg>::SharedPtr> img_subs_;
  std::vector<rclcpp::Subscription<ImuMsg>::SharedPtr> imu_subs_;
  rclcpp::node_interfaces::OnSetParametersCallbackHandle::SharedPtr param_cb_;

  std::atomic<uint64_t> enqueued_count_{0};
  std::atomic<uint64_t> dropped_count_{0};
  std::chrono::steady_clock::time_point last_report_time_;

  rclcpp::CallbackGroup::SharedPtr img_cb_group_;
  rclcpp::CallbackGroup::SharedPtr imu_cb_group_;
};

}  // namespace zed_mcap_recorder

RCLCPP_COMPONENTS_REGISTER_NODE(zed_mcap_recorder::ZedMcapRecorder)