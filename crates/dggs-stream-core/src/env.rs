//! Environment variables, read under the project's current `DGGS_STREAM_` prefix
//! with the former `RASTER_H3_` prefix as a fallback.

/// Prefix of every environment variable the engine reads.
pub const PREFIX: &str = "DGGS_STREAM_";

/// Prefix the same variables had before the project was renamed from raster_h3.
pub const LEGACY_PREFIX: &str = "RASTER_H3_";

/// The former name of `name` (`RASTER_H3_X` for `DGGS_STREAM_X`), or `None` when
/// `name` does not start with [`PREFIX`].
pub fn legacy_name(name: &str) -> Option<String> {
    name.strip_prefix(PREFIX)
        .map(|rest| format!("{LEGACY_PREFIX}{rest}"))
}

/// `lookup(name)`, falling back to `lookup` of its former name when the current
/// name is unset. Lets callers inject a lookup for tests.
pub fn var_with<F>(name: &str, lookup: F) -> Option<String>
where
    F: Fn(&str) -> Option<String>,
{
    lookup(name).or_else(|| legacy_name(name).and_then(|legacy| lookup(&legacy)))
}

/// The value of the environment variable `name` (a `DGGS_STREAM_*` name), or of
/// its former `RASTER_H3_*` name when `name` is unset. `None` when neither is set
/// or the value is not valid Unicode.
pub fn var(name: &str) -> Option<String> {
    var_with(name, |n| std::env::var(n).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_name_swaps_the_prefix() {
        assert_eq!(
            legacy_name("DGGS_STREAM_AUTH_TOKEN").as_deref(),
            Some("RASTER_H3_AUTH_TOKEN")
        );
        assert_eq!(legacy_name("AWS_SESSION_TOKEN"), None);
    }

    #[test]
    fn current_name_wins_and_former_name_is_a_fallback() {
        let both = |n: &str| match n {
            "DGGS_STREAM_X" => Some("new".to_owned()),
            "RASTER_H3_X" => Some("old".to_owned()),
            _ => None,
        };
        assert_eq!(var_with("DGGS_STREAM_X", both).as_deref(), Some("new"));
        let legacy_only = |n: &str| (n == "RASTER_H3_X").then(|| "old".to_owned());
        assert_eq!(
            var_with("DGGS_STREAM_X", legacy_only).as_deref(),
            Some("old")
        );
        assert_eq!(var_with("DGGS_STREAM_X", |_| None), None);
        // Names outside the prefix have no fallback.
        let other = |n: &str| (n == "RASTER_H3_Y").then(|| "old".to_owned());
        assert_eq!(var_with("AWS_Y", other), None);
    }
}
