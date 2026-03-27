# zed_mcap_recorder

ROS 2 component package for recording ZED camera compressed image and IMU topics into MCAP files using `rosbag2_cpp`.

## Features

- Component node: `zed_mcap_recorder::ZedMcapRecorder`
- Asynchronous writer thread to reduce callback blocking
- Runtime start/stop recording via parameter switch
- Per-topic subscription setup for compressed image and IMU topics

## Build

```bash
colcon build --packages-select zed_mcap_recorder
```

## Run

```bash
ros2 run zed_mcap_recorder zed_mcap_recorder_node \
  --ros-args \
  -p bag_path:=/tmp/zed_recordings/ \
  -p recording:=true \
  -p topics:="['/zed/zed_node/left/image_rect_color/compressed','/zed/zed_node/imu/data']"
```

## Parameters

- `bag_path` (string): Output directory prefix for bag files.
- `recording` (bool): Start/stop recording.
- `queue_depth_image` (int): QoS depth for image topics (default: 60).
- `queue_depth_imu` (int): QoS depth for IMU topics (default: 200).
- `topics` (string array): Topics to subscribe and record.

## Notes

- Topic message type is inferred from topic name:
  - contains `imu` -> `sensor_msgs/msg/Imu`
  - contains `compressed` -> `sensor_msgs/msg/CompressedImage`
- Storage backend is set to MCAP (`storage_id = mcap`).
