//! Custom `tracing` layer that mirrors info+ events into a `LogBuffer` so the
//! dashboard can tail recent log lines.

use std::fmt::Write as _;

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

use crate::state::LogBuffer;

pub struct DashboardLogLayer {
    buf: LogBuffer,
}

impl DashboardLogLayer {
    pub fn new(buf: LogBuffer) -> Self {
        Self { buf }
    }
}

struct MessageVisitor {
    out: String,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.out, "{:?}", value);
        } else {
            if !self.out.is_empty() {
                self.out.push(' ');
            }
            let _ = write!(self.out, "{}={:?}", field.name(), value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.out.push_str(value);
        } else {
            if !self.out.is_empty() {
                self.out.push(' ');
            }
            let _ = write!(self.out, "{}={}", field.name(), value);
        }
    }
}

impl<S> Layer<S> for DashboardLogLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        // Only capture INFO / WARN / ERROR for the dashboard tail (DEBUG/TRACE
        // would flood the buffer and most aren't useful to operators).
        let level = *meta.level();
        if level > tracing::Level::INFO {
            return;
        }
        let mut visitor = MessageVisitor { out: String::new() };
        event.record(&mut visitor);
        if visitor.out.is_empty() {
            return;
        }
        self.buf
            .push(meta.level().as_str(), meta.target(), visitor.out);
    }
}
