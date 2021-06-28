use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use std::{
    collections::HashMap,
    fs::{self, read_to_string},
    str,
    time::{SystemTime, UNIX_EPOCH},
};

type Segment = Map<String, Value>;
fn main() {
    println!("Starting app");

    // Load X12 message from plain text file
    let x12 = read_to_string("./sample_message.txt").unwrap();

    // Split message into segments (split by segment delimiter and then data delimiter)
    let mut segments = prepare_segments(&x12);

    let mut all_loops = get_schema_for_msg(&segments);
    let root_elements = all_loops.remove("root").unwrap().elements;

    let start = now_in_ms();

    let (map, _) = parse_message(&mut segments, &root_elements, &all_loops, 0);

    let end = now_in_ms();

    fs::write("./output.json", serde_json::to_string_pretty(&map).unwrap()).unwrap();

    println!("Total processing time (in ms): {:?}", end - start);
}

fn now_in_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn parse_message(
    raw_segments: &mut Vec<Vec<String>>,
    schema_elements: &Vec<SchemaElement>,
    all_loops: &SchemaMessage,
    mut segment_index: usize,
) -> (Map<String, Value>, usize) {
    let mut parsed: Map<String, Value> = Map::new();
    let mut schema_cursor: usize = 0;

    loop {
        let (current_element, current_segment) = if let (Some(a), Some(b)) = (
            schema_elements.get(schema_cursor),
            raw_segments.get(segment_index),
        ) {
            (a, b)
        } else {
            break;
        };

        // Current schema element is a segment
        if current_element.segment != "" {
            // Segment is match??
            if current_segment[0] != current_element.segment
                || (current_segment[0] == "HL"
                    && current_element.level != ""
                    && current_segment[3] != current_element.level)
            {
                schema_cursor = schema_cursor + 1;
                continue;
            }

            build_message(
                current_element,
                make_segment(current_segment.to_owned()),
                &mut parsed,
            );

            segment_index = segment_index + 1;

            if current_element.max == 1 {
                schema_cursor = schema_cursor + 1;
            }

            continue;
        }

        // Current schema element is a loop - need to recurse
        if current_element.r#loop != "" {
            let loop_elements = &all_loops[&current_element.r#loop].elements;
            let (parsed_loop, new_seg_index) =
                parse_message(raw_segments, loop_elements, all_loops, segment_index);

            segment_index = new_seg_index;
            if parsed_loop.len() < 1 {
                schema_cursor = schema_cursor + 1;
                continue;
            }

            build_message(current_element, parsed_loop, &mut parsed);
            if current_element.max == 1 {
                schema_cursor = schema_cursor + 1;
            }

            continue;
        }
    }

    return (parsed, segment_index);
}

fn build_message(
    definition: &SchemaElement,
    parsed_value: Map<String, Value>,
    parsed_message: &mut Map<String, Value>,
) {
    let mut name = &definition.segment;
    if name == "" {
        name = &definition.r#loop;
    }

    let max = definition.max;

    if max != 1 {
        if parsed_message.contains_key(name) && parsed_message[name].is_array() {
            // println!("zzpushing");
            parsed_message
                .get_mut(name)
                .unwrap()
                .as_array_mut()
                .unwrap()
                .push(Value::Object(parsed_value));
        /*             parsed_message[name]
        .as_array_mut()
        .unwrap()
        .push(parsed_value); */
        } else {
            // println!("zzcreating");
            // parsed_message[name] = json!([parsed_value]);
            // parsed_message[name] = Value::Array(vec![parsed_value]);
            parsed_message.insert(
                name.to_string(),
                Value::Array(vec![Value::Object(parsed_value)]),
            );
            // println!("is array {}", parsed_message[name].is_array());
        }
        return;
    }

    parsed_message.insert(name.to_string(), Value::Object(parsed_value));
    //parsed_message[name] = parsed_value;
}

fn make_segment(segment: Vec<String>) -> Segment {
    let mut seg_map: Map<String, Value> = Map::new();

    let mut i = 0;
    for value in segment {
        if i == 0 {
            i = i + 1;
            continue;
        }
        seg_map.insert((i).to_string(), Value::String(value));
        i = i + 1;
    }

    return seg_map;
}

fn prepare_segments(x12: &str) -> Vec<Vec<String>> {
    let delim_segment = "~\n";
    let delim_data = "*";

    let lines = x12.split(delim_segment);

    let segs: Vec<Vec<String>> = lines
        .map(|line: &str| {
            line.split(delim_data)
                .map(|line| line.to_string())
                .collect()
        })
        .collect();

    return segs;
}

fn default_val() -> String {
    "".to_string()
}

fn default_bool() -> bool {
    return false;
}

fn default_max() -> i32 {
    return 0;
}

#[derive(Serialize, Deserialize, Debug)]
struct SchemaElement {
    #[serde(default = "default_val")]
    segment: String,
    #[serde(default = "default_bool")]
    required: bool,
    #[serde(default = "default_max")]
    max: i32,
    #[serde(default = "default_val")]
    r#loop: String,
    #[serde(default = "default_val")]
    description: String,
    #[serde(default = "default_val")]
    level: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct SchemaLoop {
    elements: Vec<SchemaElement>,
}
type SchemaMessage = HashMap<String, SchemaLoop>;
type SchemaCollection = HashMap<String, SchemaMessage>;

fn get_schema_for_msg(segments: &Vec<Vec<String>>) -> SchemaMessage {
    let st_segment = segments.get(2).unwrap();
    println!("Found ST segment {:?}", st_segment);

    if st_segment[0] != "ST" {
        panic!("No ST segment");
    }

    let message_name = &st_segment[1];
    let schema_name: String;
    if message_name == "837" {
        let reference_id = &st_segment[3];
        if reference_id.contains("005010X22") {
            schema_name = "837i".to_string();
        } else {
            schema_name = "837p".to_string();
        }
    } else {
        schema_name = message_name.to_string();
    }

    println!("Getting schema for message name {}", schema_name);
    return get_msg_schema(&schema_name);
}

fn get_msg_schema(schema_name: &str) -> SchemaMessage {
    let json_str = read_to_string("./message_schemas.json").unwrap();
    let mut v: SchemaCollection = match serde_json::from_str(&json_str) {
        Err(err) => panic!("hi {}", err),
        Ok(val) => val,
    };

    let ret = v.remove(schema_name).unwrap();

    return ret;
}
