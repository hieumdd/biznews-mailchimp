from mailchimp.pipeline import departments, employees, activities

pipelines = {
    i.name: i
    for i in [
        j.pipeline  # type: ignore
        for j in [
            departments,
            employees,
            activities,
        ]
    ]
}
