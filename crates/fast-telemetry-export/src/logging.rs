#[allow(unused_macros)]
macro_rules! log_debug {
    ($($arg:tt)+) => {{
        #[cfg(feature = "logging-debug")]
        {
            if ::eden_logger::should_log(::eden_logger::LogLevel::Debug) {
                let __ft_ctx = ::eden_logger::LogContext::<()>::new().with_feature(module_path!());
                ::eden_logger::log_debug!(
                    __ft_ctx,
                    ::std::format!($($arg)+),
                    audience = ::eden_logger::LogAudience::Internal
                );
            }
        }
        #[cfg(not(feature = "logging-debug"))]
        if false {
            let _ = ::std::format!($($arg)+);
        }
    }};
}

#[allow(unused_macros)]
macro_rules! log_error {
    ($($arg:tt)+) => {{
        #[cfg(feature = "logging")]
        {
            if ::eden_logger::should_log(::eden_logger::LogLevel::Error) {
                let __ft_ctx = ::eden_logger::LogContext::<()>::new().with_feature(module_path!());
                ::eden_logger::log_error!(
                    __ft_ctx,
                    ::std::format!($($arg)+),
                    audience = ::eden_logger::LogAudience::Internal
                );
            }
        }
        #[cfg(not(feature = "logging"))]
        if false {
            let _ = ::std::format!($($arg)+);
        }
    }};
}

#[allow(unused_macros)]
macro_rules! log_info {
    ($($arg:tt)+) => {{
        #[cfg(feature = "logging")]
        {
            if ::eden_logger::should_log(::eden_logger::LogLevel::Info) {
                let __ft_ctx = ::eden_logger::LogContext::<()>::new().with_feature(module_path!());
                ::eden_logger::log_info!(
                    __ft_ctx,
                    ::std::format!($($arg)+),
                    audience = ::eden_logger::LogAudience::Internal
                );
            }
        }
        #[cfg(not(feature = "logging"))]
        if false {
            let _ = ::std::format!($($arg)+);
        }
    }};
}

#[allow(unused_macros)]
macro_rules! log_warn {
    ($($arg:tt)+) => {{
        #[cfg(feature = "logging")]
        {
            if ::eden_logger::should_log(::eden_logger::LogLevel::Warn) {
                let __ft_ctx = ::eden_logger::LogContext::<()>::new().with_feature(module_path!());
                ::eden_logger::log_warn!(
                    __ft_ctx,
                    ::std::format!($($arg)+),
                    audience = ::eden_logger::LogAudience::Internal
                );
            }
        }
        #[cfg(not(feature = "logging"))]
        if false {
            let _ = ::std::format!($($arg)+);
        }
    }};
}

#[allow(unused_imports)]
pub(crate) use {log_debug, log_error, log_info, log_warn};
