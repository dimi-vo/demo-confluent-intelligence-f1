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

# README step 10's checks, one block per developer with their IDs filled in;
# print it with `terraform output -raw validation`. module.developer is a map,
# so it iterates in email order, like the handout. The Confluent CLI can't print
# a query's result, so the row count is SQL for the Console's Flink SQL
# workspace, fully qualified so it doesn't depend on the selected catalog.
output "validation" {
  description = "The commands and SQL that check each developer's topics and seed, one block per developer"
  value = join("\n", [
    for email, dev in module.developer : <<-EOT
      ${email}: ${dev.environment_name} (${dev.environment_id})

        1. The five statements are COMPLETED:
           confluent flink statement list --environment ${dev.environment_id} --cloud aws --region ${var.region}

        2. The topics car_telemetry, race_standings and driver_race_history exist:
           confluent kafka topic list --environment ${dev.environment_id} --cluster ${dev.cluster_id}

        3. driver_race_history holds 198 rows (Flink SQL workspace, compute pool ${dev.compute_pool_name}):
           SELECT COUNT(*) FROM `${dev.environment_name}`.`${dev.cluster_name}`.`driver_race_history`;
    EOT
  ])
}
