# Alarme, não trava (regra 7 da spec). Budget sem ações: não tem custo.
# Vale para a conta inteira: o filtro por tag exigiria ativar a tag de alocação de custo.
resource "aws_budgets_budget" "cost_guard" {
  name         = "brasileirao-cost-guard"
  budget_type  = "COST"
  limit_amount = "15"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # No Free plan o gasto é pago com crédito. Sem tirar o crédito, o custo líquido fica em zero
  # e o alarme nunca dispara.
  cost_types {
    include_credit = false
    include_refund = false
  }

  dynamic "notification" {
    for_each = [5, 10, 15]

    content {
      comparison_operator        = "GREATER_THAN"
      notification_type          = "ACTUAL"
      threshold                  = notification.value
      threshold_type             = "ABSOLUTE_VALUE"
      subscriber_email_addresses = [var.email_alerta]
    }
  }
}
