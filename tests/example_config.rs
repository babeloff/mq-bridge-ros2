//! The shipped example parses into a `ros2` endpoint. Needs no running ROS graph,
//! but building the crate needs a sourced ROS environment like every test here.

use mq_bridge::models::EndpointType;

#[test]
fn the_example_route_addresses_ros2_by_name() {
    let document: serde_yaml::Value =
        serde_yaml::from_str(include_str!("../examples/ros2_to_file.yaml")).unwrap();
    let route: mq_bridge::Route =
        serde_yaml::from_value(document["routes"]["ros2_to_file"].clone()).unwrap();

    let EndpointType::Custom { name, config } = route.input.endpoint_type else {
        panic!("the `ros2:` key must resolve to the custom ros2 endpoint");
    };
    assert_eq!(name, "ros2");
    assert_eq!(config["qos"]["depth"], 100);
}
