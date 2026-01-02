use std::env;
use std::mem;

pub struct Options {
    pub port: String,
    pub replica_options: Option<ReplicaOptions>,
}

pub struct ReplicaOptions {
    pub master_host: String,
    pub master_port: String,
}

// Panics if arg parsing goes wrong!
pub fn parse_args() -> Options {
    let mut args: Vec<String> = env::args().collect();
    let mut options = Options {
        port: "6379".to_string(),
        replica_options: None,
    };
    // The first is process name
    let mut i: usize = 1;
    while i < args.len() {
        let option = mem::take(&mut args[i]);
        i += 1;
        match option.as_str() {
            "--port" => {
                options.port = mem::take(&mut args[i]);
                i += 1;
            }
            "--replicaof" => {
                let mut master_host_port = mem::take(&mut args[i]);
                i += 1;
                let split_index = master_host_port
                    .find(' ')
                    .expect("Master host port must be separated by a space");
                let master_port = master_host_port[split_index + 1..].to_string();
                master_host_port.truncate(split_index);
                let master_host = master_host_port;
                options.replica_options = Some(ReplicaOptions {
                    master_host,
                    master_port,
                });
            }
            _ => panic!("Unexpected option {}", option),
        }
    }
    return options;
}
