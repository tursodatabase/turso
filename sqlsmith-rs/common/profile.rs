use log::info;
use serde::{Deserialize, Serialize};
use sqlsmith_rs_drivers::DRIVER_KIND;
use std::fs;

#[derive(Serialize, Deserialize, Debug)]
pub struct Profile {
    pub driver: Option<DRIVER_KIND>,
    pub count: Option<usize>,
    pub executor_count: Option<usize>,
    pub thread_per_exec: Option<usize>, // <-- Added
    pub stmt_prob: Option<StmtProb>,
    pub debug: Option<DebugOptions>,
    pub seed: Option<u64>, // Added seed field
}

#[derive(Serialize, Deserialize, Debug, Clone)] // 添加 Clone
pub struct DebugOptions {
    pub show_success_sql: bool,
    pub show_failed_sql: bool,
    pub show_sql_before_exec: bool, // 新增 show_sql_before_exec
    pub write_log_file: bool,       // 新增 write_log_file
}

#[derive(Serialize, Deserialize, Debug, Clone)] // 添加 Clone
pub struct StmtProb {
    pub DELETE: u64,
    pub SELECT: u64,
    pub INSERT: u64,
    pub UPDATE: u64,
    pub VACUUM: u64,
    pub PRAGMA: u64,
    pub CREATE_TRIGGER: u64,
    pub DROP_TRIGGER: u64, // 新增 DropTrigger 字段
    pub DATE_FUNC: u64,    // 新增 DATE_FUNC 字段
    pub ALTER_TABLE: u64,  // 新增 ALTER_TABLE 字段
    pub CREATE_TABLE: u64, // 保留 CREATE_TABLE 字段
    pub TRANSACTION: u64, // 保留 TRANSACTION 字段
}

pub fn read_profile() -> Profile {
    if let Ok(content) = fs::read_to_string("profile.json") {
        if let Ok(profile) = serde_json::from_str::<Profile>(&content) {
            return profile;
        }
    }

    // 直接使用默认值生成 Profile 结构体
    let driver = Some(DRIVER_KIND::SQLITE_IN_MEM);
    let count = Some(8);
    let executor_count = Some(5);
    let thread_per_exec = Some(5);
    let stmt_prob = Some(StmtProb {
        SELECT: 100,
        INSERT: 50,
        UPDATE: 50,
        DELETE: 20,
        VACUUM: 20,
        PRAGMA: 10,
        CREATE_TRIGGER: 10,
        DROP_TRIGGER: 10,
        DATE_FUNC: 20,
        ALTER_TABLE: 10,
        CREATE_TABLE: 10,
        TRANSACTION: 10,
    });
    let debug = Some(DebugOptions {
        show_success_sql: false,
        show_failed_sql: true,
        show_sql_before_exec: false,
        write_log_file: false, // 新增默认值
    });
    let seed = Some(0);

    let profile = Profile {
        driver,
        count,
        executor_count,
        thread_per_exec,
        stmt_prob,
        debug,
        seed,
    };

    if let Ok(json_str) = serde_json::to_string_pretty(&profile) {
        if let Err(e) = fs::write("profile.json", json_str) {
            eprintln!("Failed to write profile.json: {}", e);
        }
    }
    info!("default profile.json created");
    profile
}

pub fn write_profile(profile: &Profile) -> Result<(), std::io::Error> {
    let json_str = serde_json::to_string_pretty(profile)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    fs::write("profile.json", json_str)?;
    Ok(())
}

impl Profile {
    pub fn print(&self) {
        let mut items = vec![];
        items.push(format!(
            "driver={:?}",
            self.driver.unwrap_or(DRIVER_KIND::SQLITE_IN_MEM)
        ));
        items.push(format!("count={}", self.count.unwrap_or(8)));
        items.push(format!(
            "executor_count={}",
            self.executor_count.unwrap_or(5)
        ));
        items.push(format!(
            "thread_per_exec={}",
            self.thread_per_exec.unwrap_or(5)
        ));
        if let Some(seed) = self.seed {
            items.push(format!("seed={}", seed));
        }
        if let Some(stmt_prob) = &self.stmt_prob {
            items.push(format!("SELECT={}", stmt_prob.SELECT));
            items.push(format!("INSERT={}", stmt_prob.INSERT));
            items.push(format!("UPDATE={}", stmt_prob.UPDATE));
            items.push(format!("VACUUM={}", stmt_prob.VACUUM));
            items.push(format!("DELETE={}", stmt_prob.DELETE));
            items.push(format!("PRAGMA={}", stmt_prob.PRAGMA));
            items.push(format!("CREATE_TRIGGER={}", stmt_prob.CREATE_TRIGGER));
            items.push(format!("DROP_TRIGGER={}", stmt_prob.DROP_TRIGGER));
            items.push(format!("DATE_FUNC={}", stmt_prob.DATE_FUNC));
            items.push(format!("ALTER_TABLE={}", stmt_prob.ALTER_TABLE));
            items.push(format!("CREATE_TABLE={}", stmt_prob.CREATE_TABLE));
            items.push(format!("TRANSACTION={}", stmt_prob.TRANSACTION));
        }
        if let Some(debug) = &self.debug {
            items.push(format!("show_success_sql={}", debug.show_success_sql));
            items.push(format!("show_failed_sql={}", debug.show_failed_sql));
            items.push(format!("show_sql_before_exec={}", debug.show_sql_before_exec));
            items.push(format!("write_log_file={}", debug.write_log_file)); // 新增打印
        }
        log::info!("Profile: {}", items.join(", "));
    }
}
