locals {
  # Header row first, then one row per developer, sorted by email.
  handout_table = concat(
    [{ email = "EMAIL", name = "ENVIRONMENT", id = "ENVIRONMENT ID", url = "CONSOLE URL" }],
    [
      for email in sort(local.emails) : {
        email = email
        name  = module.developer[email].environment_name
        id    = module.developer[email].environment_id
        url   = "https://confluent.cloud/environments/${module.developer[email].environment_id}"
      }
    ],
  )

  handout_widths = {
    for col in ["email", "name", "id"] :
    col => max([for row in local.handout_table : length(row[col])]...)
  }
}

# One padded line per row, so the columns line up as a table; print it with
# `terraform output -raw handout`. Send each developer their row; the deploy
# finds the environment by itself, so the ID is for the platform team's records.
output "handout" {
  description = "Email, environment name, environment ID and Console URL, one line per developer"
  value = join("", [
    for row in local.handout_table :
    format(
      "%-${local.handout_widths.email}s  %-${local.handout_widths.name}s  %-${local.handout_widths.id}s  %s\n",
      row.email, row.name, row.id, row.url,
    )
  ])
}

# README step 10's check: a heading, then one numbered Console link per
# developer to their driver_race_history topic, opened by hand. Print it with
# `terraform output -raw validation`. keys() lists the emails in lexical order,
# like the handout, and its index numbers them.
output "validation" {
  description = "A numbered Console link per developer to their driver_race_history topic"
  value = join("", concat(
    ["Check via the console that each topic exists and holds the expected data:\n"],
    [
      for i, email in keys(module.developer) : format(
        "\n%d. %s: %s (%s)\n   https://confluent.cloud/environments/%s/clusters/%s/topics/driver_race_history/message-viewer?tab=schema\n",
        i + 1,
        email,
        module.developer[email].environment_name,
        module.developer[email].environment_id,
        module.developer[email].environment_id,
        module.developer[email].cluster_id,
      )
    ],
  ))
}
