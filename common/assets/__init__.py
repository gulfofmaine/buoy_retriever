import dagster as dg


def auto_condition_eager_allow_missing():
    """Allow missing upstream partitions but still eagerly execute"""
    return (
        dg.AutomationCondition.eager()
        .without(~dg.AutomationCondition.any_deps_missing())
        .without(dg.AutomationCondition.in_latest_time_window())
        .with_label("eager_allow_missing")
    )
