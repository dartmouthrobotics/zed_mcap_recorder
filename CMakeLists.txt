cmake_minimum_required(VERSION 3.8)
project(zed_mcap_recorder)

find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rclcpp_components REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(rosbag2_cpp REQUIRED)
find_package(rosbag2_storage REQUIRED)

add_library(zed_mcap_recorder SHARED src/zed_mcap_recorder.cpp)

target_include_directories(zed_mcap_recorder PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

ament_target_dependencies(zed_mcap_recorder
  rclcpp rclcpp_components sensor_msgs rosbag2_cpp rosbag2_storage)

rclcpp_components_register_node(zed_mcap_recorder
  PLUGIN "zed_mcap_recorder::ZedMcapRecorder"
  EXECUTABLE zed_mcap_recorder_node)

install(TARGETS zed_mcap_recorder
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION lib/${PROJECT_NAME})

ament_package()
