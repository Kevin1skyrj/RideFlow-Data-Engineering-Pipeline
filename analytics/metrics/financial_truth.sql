/*
    Q4  FINANCIAL TRUTH - revenue reconciled to the cent.

    Two distinct facts, deliberately kept apart:

      fct_trips     what was CHARGED  (fare recognised)
      fct_payments  what was COLLECTED (money received)

    They are not the same number and must not be merged. A completed trip with
    a quarantined payment produces revenue in one and nothing in the other, and
    a business reading only the first can be profitable and insolvent at once.

    Aggregate-then-join again: joining the two facts row-by-row through shared
    dimensions would fan out.
*/

with charged as (

    select
        request_date_id                                             as date_id,
        count(*)                                                    as completed_trips,
        round(sum(total_fare), 2)                                   as gross_bookings,
        round(sum(base_fare + distance_fare + time_fare), 2)        as base_revenue,
        round(sum(surge_amount), 2)                                 as surge_revenue,
        round(sum(airport_fee + toll_amount + booking_fee), 2)      as fees_revenue,
        round(sum(tax_amount), 2)                                   as tax_collected,
        round(sum(driver_payout), 2)                                as driver_payout,
        -- Net revenue: what the business actually earns, before gateway cost.
        round(sum(platform_commission), 2)                          as platform_commission
    from read_parquet('data/processed/fct_trips.parquet')
    where not is_quarantined and is_completed
    group by 1

),

collected as (

    select
        paid_date_id                                                as date_id,
        count(*)                                                    as payments,
        round(sum(amount_charged), 2)                               as amount_collected,
        round(sum(tip_amount), 2)                                   as tips,
        round(sum(discount_amount), 2)                              as discounts,
        -- Real margin leakage, invisible in fare data. The spread between CARD
        -- (2.00%) and UPI (0.30%) is a material profitability lever at scale.
        round(sum(gateway_fee_amount), 2)                           as gateway_fees
    from read_parquet('data/processed/fct_payments.parquet')
    group by 1

)

select
    d.full_date,
    d.day_name,
    d.is_weekend,

    c.completed_trips,
    c.gross_bookings,
    c.base_revenue,
    c.surge_revenue,
    c.fees_revenue,
    c.tax_collected,
    c.driver_payout,
    c.platform_commission,

    p.payments,
    p.amount_collected,
    p.tips,
    p.discounts,
    p.gateway_fees,

    -- Net revenue AFTER the costs that fare data hides: gateway fees are a real
    -- cost, and discounts are platform-funded (the driver is still paid on the
    -- full fare), so both reduce margin without touching driver_payout.
    round(c.platform_commission - coalesce(p.gateway_fees, 0)
          - coalesce(p.discounts, 0), 2)                            as net_platform_revenue,

    /*
        RECONCILIATION (invariant N3).

        Non-zero here is not automatically an error: it is the fare of trips
        whose payment or completion was quarantined in the DLQ. What matters is
        that it is MEASURED rather than silently absorbed into a total. A drift
        that grows over time means the DLQ rate is rising upstream.
    */
    round(c.gross_bookings - coalesce(p.amount_collected, 0)
          + coalesce(p.tips, 0) - coalesce(p.discounts, 0), 2)      as charged_vs_collected_drift

from charged as c
left join collected as p using (date_id)
join read_parquet('data/processed/dim_date.parquet') as d using (date_id)
order by d.full_date
