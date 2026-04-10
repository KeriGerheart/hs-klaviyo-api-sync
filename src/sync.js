import "dotenv/config";

const {
    HUBSPOT_TOKEN,
    KLAVIYO_API_KEY,
    KLAVIYO_REVISION,
    HUBSPOT_LIST_ID,
    KLAVIYO_LIST_ID,
    NURTURE_PROPERTY = "nurture",
    INDUSTRY_TYPE_PROPERTY = "industry_type",
    INDUSTRY_TYPE_WEB_PROPERTY = "industry_type_web",
} = process.env;

if (!HUBSPOT_TOKEN || !KLAVIYO_API_KEY || !KLAVIYO_REVISION || !HUBSPOT_LIST_ID || !KLAVIYO_LIST_ID) {
    throw new Error("Missing required env vars.");
}

const HUBSPOT_BASE = "https://api.hubapi.com";
const KLAVIYO_BASE = "https://a.klaviyo.com/api";

function chunk(array, size) {
    const out = [];
    for (let i = 0; i < array.length; i += size) out.push(array.slice(i, i + size));
    return out;
}

async function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function hubspotFetch(path, options = {}) {
    const res = await fetch(`${HUBSPOT_BASE}${path}`, {
        ...options,
        headers: {
            Authorization: `Bearer ${HUBSPOT_TOKEN}`,
            "Content-Type": "application/json",
            ...(options.headers || {}),
        },
    });

    if (!res.ok) {
        const text = await res.text();
        throw new Error(`HubSpot ${res.status} ${path}: ${text}`);
    }

    return res.json();
}

async function klaviyoFetch(path, options = {}) {
    const res = await fetch(`${KLAVIYO_BASE}${path}`, {
        ...options,
        headers: {
            Authorization: `Klaviyo-API-Key ${KLAVIYO_API_KEY}`,
            revision: KLAVIYO_REVISION,
            "Content-Type": "application/json",
            Accept: "application/json",
            ...(options.headers || {}),
        },
    });

    if (!res.ok) {
        const text = await res.text();
        throw new Error(`Klaviyo ${res.status} ${path}: ${text}`);
    }

    if (res.status === 204) return null;
    return res.json();
}

async function getOwnersMap() {
    const data = await hubspotFetch("/crm/v3/owners");
    const map = new Map();

    for (const owner of data.results || []) {
        const label =
            [owner.firstName, owner.lastName].filter(Boolean).join(" ").trim() || owner.email || String(owner.id);

        map.set(String(owner.id), {
            id: String(owner.id),
            name: label,
            email: owner.email || null,
        });
    }

    return map;
}

async function getHubSpotListMemberIds(listId) {
    let after = null;
    const ids = [];

    do {
        const query = new URLSearchParams();
        query.set("limit", "100");
        if (after) query.set("after", after);

        const data = await hubspotFetch(`/crm/v3/lists/${listId}/memberships?${query.toString()}`);
        for (const row of data.results || []) {
            // Lists v3 memberships return record references; adapt if your payload shape differs.
            if (row.recordId) ids.push(String(row.recordId));
            else if (row.record?.id) ids.push(String(row.record.id));
            else if (row.id) ids.push(String(row.id));
        }

        after = data.paging?.next?.after || null;
    } while (after);

    return ids;
}

async function getContact(contactId) {
    const params = new URLSearchParams();
    [
        "email",
        "firstname",
        "lastname",
        "hubspot_owner_id",
        NURTURE_PROPERTY,
        INDUSTRY_TYPE_PROPERTY,
        INDUSTRY_TYPE_WEB_PROPERTY,
    ].forEach((p) => params.append("properties", p));

    return hubspotFetch(`/crm/v3/objects/contacts/${contactId}?${params.toString()}`);
}

async function getAssociations(fromObjectType, fromId, toObjectType) {
    const data = await hubspotFetch(`/crm/v4/objects/${fromObjectType}/${fromId}/associations/${toObjectType}`);
    return data.results || [];
}

async function getDeal(dealId) {
    const params = new URLSearchParams();
    ["dealname", "hubspot_owner_id"].forEach((p) => params.append("properties", p));
    return hubspotFetch(`/crm/v3/objects/deals/${dealId}?${params.toString()}`);
}

async function getCompany(companyId) {
    const params = new URLSearchParams();
    ["name", "hubspot_owner_id"].forEach((p) => params.append("properties", p));
    return hubspotFetch(`/crm/v3/objects/companies/${companyId}?${params.toString()}`);
}

function ownerLabel(ownerId, ownersMap) {
    if (!ownerId) return null;
    return ownersMap.get(String(ownerId))?.name || String(ownerId);
}

async function buildNormalizedRecord(contactId, ownersMap) {
    const contact = await getContact(contactId);
    const contactProps = contact.properties || {};
    const email = contactProps.email?.trim();

    if (!email) {
        return { skip: true, reason: "missing_email", contactId };
    }

    const dealAssoc = await getAssociations("contacts", contactId, "deals");
    const companyAssoc = await getAssociations("contacts", contactId, "companies");

    const firstDealId = dealAssoc[0]?.toObjectId ? String(dealAssoc[0].toObjectId) : null;
    const firstCompanyId = companyAssoc[0]?.toObjectId ? String(companyAssoc[0].toObjectId) : null;

    const deal = firstDealId ? await getDeal(firstDealId) : null;
    const company = firstCompanyId ? await getCompany(firstCompanyId) : null;

    return {
        email,
        contactId: String(contact.id),
        companyId: company?.id ? String(company.id) : null,
        dealId: deal?.id ? String(deal.id) : null,
        nurture: contactProps[NURTURE_PROPERTY] ?? null,
        industryType: contactProps[INDUSTRY_TYPE_PROPERTY] ?? null,
        industryTypeWeb: contactProps[INDUSTRY_TYPE_WEB_PROPERTY] ?? null,

        contactOwnerId: contactProps.hubspot_owner_id || null,
        contactOwner: ownerLabel(contactProps.hubspot_owner_id, ownersMap),

        dealName: deal?.properties?.dealname || null,
        dealOwnerId: deal?.properties?.hubspot_owner_id || null,
        dealOwner: ownerLabel(deal?.properties?.hubspot_owner_id, ownersMap),

        companyOwnerId: company?.properties?.hubspot_owner_id || null,
        companyOwner: ownerLabel(company?.properties?.hubspot_owner_id, ownersMap),
    };
}

function toKlaviyoProfile(record) {
    return {
        type: "profile",
        attributes: {
            email: record.email,
            properties: {
                hubspot_nurture: record.nurture,
                hubspot_industry_type: record.industryType,
                hubspot_industry_type_web: record.industryTypeWeb,
                hubspot_contact_owner: record.contactOwner,
                hubspot_contact_owner_id: record.contactOwnerId,
                hubspot_deal_name: record.dealName,
                hubspot_deal_owner: record.dealOwner,
                hubspot_deal_owner_id: record.dealOwnerId,
                hubspot_company_owner: record.companyOwner,
                hubspot_company_owner_id: record.companyOwnerId,
                hubspot_contact_id: record.contactId,
                hubspot_deal_id: record.dealId,
                hubspot_company_id: record.companyId,
            },
        },
    };
}

async function bulkImportProfiles(profiles) {
    const data = await klaviyoFetch("/profile-bulk-import-jobs", {
        method: "POST",
        body: JSON.stringify({
            data: {
                type: "profile-bulk-import-job",
                attributes: {
                    profiles: {
                        data: profiles,
                    },
                },
            },
        }),
    });

    return data?.data?.id || null;
}

async function getKlaviyoProfileIdByEmail(email) {
    const filter = encodeURIComponent(`equals(email,"${email}")`);
    const data = await klaviyoFetch(`/profiles?filter=${filter}`);
    return data?.data?.[0]?.id || null;
}

async function addProfilesToKlaviyoList(profileIds) {
    const chunks = chunk(profileIds, 1000);

    for (const ids of chunks) {
        await klaviyoFetch(`/lists/${KLAVIYO_LIST_ID}/relationships/profiles`, {
            method: "POST",
            body: JSON.stringify({
                data: ids.map((id) => ({
                    type: "profile",
                    id,
                })),
            }),
        });
    }
}

async function getBulkImportJob(jobId) {
    return klaviyoFetch(`/profile-bulk-import-jobs/${jobId}`);
}

async function waitForBulkImportJob(jobId, options = {}) {
    const { maxAttempts = 24, intervalMs = 5000 } = options;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        const job = await getBulkImportJob(jobId);
        const attrs = job?.data?.attributes || {};
        const status = String(attrs.status || "unknown").toLowerCase();

        console.log(`Bulk import job ${jobId} status: ${status} (attempt ${attempt}/${maxAttempts})`);

        if (status === "complete" || status === "completed") {
            return job;
        }

        if (status === "failed" || status === "cancelled") {
            throw new Error(`Bulk import job ${jobId} ended with status: ${status}`);
        }

        if (attempt < maxAttempts) {
            await sleep(intervalMs);
        }
    }

    throw new Error(`Timed out waiting for bulk import job ${jobId} to complete.`);
}

async function main() {
    console.log("Loading HubSpot owners...");
    const ownersMap = await getOwnersMap();

    console.log("Loading HubSpot list members...");
    const contactIds = await getHubSpotListMemberIds(HUBSPOT_LIST_ID);
    console.log(`Found ${contactIds.length} member(s) in HubSpot list.`);

    const normalized = [];
    const skipped = [];

    const stats = {
        missingEmail: 0,
        missingDeal: 0,
        missingCompany: 0,
        missingContactOwner: 0,
        missingDealOwner: 0,
        missingCompanyOwner: 0,
    };

    for (const contactId of contactIds) {
        try {
            const record = await buildNormalizedRecord(contactId, ownersMap);

            if (record.skip) {
                skipped.push(record);

                if (record.reason === "missing_email") {
                    stats.missingEmail += 1;
                    console.warn(`Skipping ${contactId}: missing email`);
                }

                continue;
            }

            if (!record.dealId) {
                stats.missingDeal += 1;
                console.log(`No deal for ${record.email}`);
            }

            if (!record.companyId) {
                stats.missingCompany += 1;
                console.log(`No company for ${record.email}`);
            }

            if (!record.contactOwnerId) {
                stats.missingContactOwner += 1;
                console.log(`No contact owner for ${record.email}`);
            }

            if (record.dealId && !record.dealOwnerId) {
                stats.missingDealOwner += 1;
                console.log(`No deal owner for ${record.email}`);
            }

            if (record.companyId && !record.companyOwnerId) {
                stats.missingCompanyOwner += 1;
                console.log(`No company owner for ${record.email}`);
            }

            normalized.push(record);
            console.log(`Prepared ${record.email}`);
            await sleep(100);
        } catch (err) {
            console.error(`Failed on contact ${contactId}:`, err.message);
            skipped.push({ contactId, reason: "build_failed", error: err.message });
        }
    }

    console.log(`Prepared ${normalized.length} record(s), skipped ${skipped.length}.`);

    if (!normalized.length) {
        console.log("No valid records to sync.");
        return;
    }

    const profilePayload = normalized.map(toKlaviyoProfile);

    console.log("Bulk importing profiles into Klaviyo...");
    const jobId = await bulkImportProfiles(profilePayload);
    console.log("Bulk import job:", jobId);

    if (!jobId) {
        throw new Error("Klaviyo bulk import did not return a job ID.");
    }

    await waitForBulkImportJob(jobId);

    console.log("Resolving Klaviyo profile IDs...");
    const profileIds = [];
    for (const record of normalized) {
        const profileId = await getKlaviyoProfileIdByEmail(record.email);
        if (profileId) {
            profileIds.push(profileId);
            console.log(`Resolved ${record.email} -> ${profileId}`);
        } else {
            console.warn(`No Klaviyo profile found yet for ${record.email}`);
        }
        await sleep(100);
    }

    if (profileIds.length) {
        console.log(`Adding ${profileIds.length} profile(s) to Klaviyo list...`);
        await addProfilesToKlaviyoList(profileIds);
    }

    console.log("Done.");
    console.log({
        hubspotMembers: contactIds.length,
        prepared: normalized.length,
        skipped,
        klaviyoProfilesAdded: profileIds.length,
    });
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
