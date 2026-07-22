macro_rules! log_debug {
    ($($arg:tt)+) => {{
        if ::eden_logger::should_log(::eden_logger::LogLevel::Debug) {
            let __ft_ctx = ::eden_logger::LogContext::<()>::new().with_feature(module_path!());
            ::eden_logger::log_debug!(
                __ft_ctx,
                ::std::format!($($arg)+),
                audience = ::eden_logger::LogAudience::Internal
            );
        }
    }};
}

macro_rules! log_error {
    ($($arg:tt)+) => {{
        if ::eden_logger::should_log(::eden_logger::LogLevel::Error) {
            let __ft_ctx = ::eden_logger::LogContext::<()>::new().with_feature(module_path!());
            ::eden_logger::log_error!(
                __ft_ctx,
                ::std::format!($($arg)+),
                audience = ::eden_logger::LogAudience::Internal
            );
        }
    }};
}

macro_rules! log_info {
    ($($arg:tt)+) => {{
        if ::eden_logger::should_log(::eden_logger::LogLevel::Info) {
            let __ft_ctx = ::eden_logger::LogContext::<()>::new().with_feature(module_path!());
            ::eden_logger::log_info!(
                __ft_ctx,
                ::std::format!($($arg)+),
                audience = ::eden_logger::LogAudience::Internal
            );
        }
    }};
}

macro_rules! log_warn {
    ($($arg:tt)+) => {{
        if ::eden_logger::should_log(::eden_logger::LogLevel::Warn) {
            let __ft_ctx = ::eden_logger::LogContext::<()>::new().with_feature(module_path!());
            ::eden_logger::log_warn!(
                __ft_ctx,
                ::std::format!($($arg)+),
                audience = ::eden_logger::LogAudience::Internal
            );
        }
    }};
}

pub(crate) use {log_debug, log_error, log_info, log_warn};
