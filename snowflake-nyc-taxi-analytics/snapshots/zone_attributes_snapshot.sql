{% snapshot zone_attributes_snapshot %}

  {{
    config(
      target_schema = 'snapshots',
      unique_key    = 'zone_id',
      strategy      = 'check',
      check_cols    = ['borough', 'service_zone']
    )
  }}

  select
      zone_id,
      borough,
      zone_name,
      service_zone
  from {{ ref('taxi_zones') }}

{% endsnapshot %}
