#[derive(PartialEq, Debug, Clone)]
pub struct DBInclusionRule {
    pub include: bool,
    pub rule: glob::Pattern,
}
