# HubSpot → Klaviyo Sync

Syncs contacts from a HubSpot list into a Klaviyo list and updates Klaviyo profile properties with HubSpot-derived data.

## What it syncs

For each contact in the configured HubSpot list, the script:

- reads the HubSpot contact
- reads associated deal and company data
- resolves HubSpot owner IDs to names
- updates the Klaviyo profile with:
    - `hubspot_nurture`
    - `hubspot_industry`
    - `hubspot_contact_owner`
    - `hubspot_contact_owner_id`
    - `hubspot_deal_name`
    - `hubspot_deal_owner`
    - `hubspot_deal_owner_id`
    - `hubspot_company_owner`
    - `hubspot_company_owner_id`
    - `hubspot_contact_id`
    - `hubspot_deal_id`
    - `hubspot_company_id`
- adds the profile to the configured Klaviyo list

## Environment variables

Required:

- `HUBSPOT_TOKEN`
- `KLAVIYO_API_KEY`
- `KLAVIYO_REVISION`
- `HUBSPOT_LIST_ID`
- `KLAVIYO_LIST_ID`

Optional:

- `NURTURE_PROPERTY` (defaults to `nurture`)

## Local development

Install dependencies:

```bash
npm install
```
