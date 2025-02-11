use clap::{Parser, ValueEnum};

#[derive(Parser, Debug)]
pub struct Opt {
    /// The name of the camera. Must be a name in the config
    pub camera: String,
    /// The action to perform
    #[command(subcommand)]
    pub cmd: UserAction,
}

#[derive(Parser, Debug)]
pub enum UserAction {
    /// List users
    List,
    /// Create a new user
    Add {
        /// The username of the new user
        user_name: String,
        /// The password of the new user
        password: String,
        /// User type
        user_type: UserType,
    },
    Password {
        /// The username of the new user
        user_name: String,
        /// The password of the new user
        password: String,
    },
    Delete {
        /// The username of the user to delete
        user_name: String,
    },
}

#[derive(Parser, Debug, Clone, ValueEnum)]
pub enum UserType {
    /// user_level = 0
    User,
    /// user_level = 1
    Administrator,
}
