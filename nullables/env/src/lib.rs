use std::env::VarError;

pub struct Env {
    is_nulled: bool,
    configured_responses: Vec<(&'static str, &'static str)>,
}

impl Env {
    pub fn var(&self, key: impl AsRef<str>) -> Result<String, VarError> {
        let key = key.as_ref();
        if self.is_nulled {
            self.configured_responses
                .iter()
                .find_map(|(k, v)| if *k == key { Some(v.to_string()) } else { None })
                .ok_or(VarError::NotPresent)
        } else {
            std::env::var(key)
        }
    }
}

impl Env {
    pub fn new_null() -> Self {
        Self {
            is_nulled: true,
            configured_responses: Vec::new(),
        }
    }

    pub fn new_null_with(configured_responses: Vec<(&'static str, &'static str)>) -> Self {
        Self {
            is_nulled: true,
            configured_responses,
        }
    }
}

impl Default for Env {
    fn default() -> Self {
        Self {
            is_nulled: false,
            configured_responses: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_real_env_var() {
        let (key, value) = std::env::vars().next().expect("no env vars found");
        assert_eq!(Env::default().var(key).unwrap(), value);
    }

    mod nullability {
        use super::*;

        #[test]
        fn can_be_nulled() {
            assert_eq!(Env::new_null().var("PATH"), Err(VarError::NotPresent));
        }

        #[test]
        fn returns_configured_responses() {
            let env = Env::new_null_with(vec![("foo", "bar"), ("foo2", "bar2")]);
            assert_eq!(env.var("foo"), Ok("bar".to_string()));
            assert_eq!(env.var("foo2"), Ok("bar2".to_string()));
            assert_eq!(env.var("foo3"), Err(VarError::NotPresent));
        }
    }
}
