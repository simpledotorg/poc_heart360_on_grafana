# HEARTS360 — User Access Setup Guide

A plain guide to add users and control what they can see in HEARTS360.

---

## 30-Second Summary

> Log in as admin → Create user → Add to a team → User logs in. Done.

- A **login** lets the person open HEARTS360
- A **team** controls which facility's data they can see

---

## What Dashboards Exist?

```
HEARTS360
│
├── 🌐 GLOBAL Dashboards             (open to anyone logged in)
│   ├── Hypertension
│   ├── Diabetes
│   └── Overdue Patients
│
└── 🔒 FACILITY SECURE Dashboards    (team controls who sees what)
    ├── Hypertension (Secure)
    ├── Diabetes (Secure)
    └── Overdue Patients (Secure)
```

| User type | Sees Global dashboard? | Sees Secure Dashboard? |
|---|---|---|
| Not logged in | ✗ | ✗ |
| Logged in, no team | ✔ | ⚠ Access Denied |
| Logged in, on one facility's team | ✔ | ✔ that facility only |
| Logged in, on `_ALL` team | ✔ | ✔ every facility |
| Admin | ✔ | ✔ every facility |

---

## How Team Names Work

Every facility team follows the same naming pattern:

```
heart360tk_facility_view_<TYPE>_<FACILITY_SLUG>
                          ▲       ▲
                          │       └─ Facility name in lowercase
                          │          spaces replaced with underscore
                          │
                          └─ "patients"   = (HTN + DM) graphs access + overdue patient list
                             "aggregated" = (HTN + DM) graphs access (no overdue patient list access)
```

### How to get the facility slug

Take the facility name, which is visible in the facility dropdown → make it lowercase → replace spaces with `_`.

| Facility name | Slug |
|---|---|
| PHC Garden | `phc_garden` |
| PHC Sunrise | `phc_sunrise` |
| Rau CHC | `rau_chc` |

### Real team-name examples

| Goal | Team name |
|---|---|
| Nurse at PHC Garden, full access | `heart360tk_facility_view_patients_phc_garden` |
| Researcher at PHC Garden, (HTN + DM) graphs access only | `heart360tk_facility_view_aggregated_phc_garden` |
| Regional supervisor — all facilities, full access | `heart360tk_facility_view_patients_ALL` |
| Regional supervisor — all facilities, (HTN + DM) graphs access only | `heart360tk_facility_view_aggregated_ALL` |

---

## Step-by-step: Add a New User

### Step 1 — Create the user

1. Log in as Admin.
2. Sidebar → **Administration → Users**
3. Click **New user**
4. Fill in name, email, username, password
5. Click **Create user**

### Step 2 — Give one-facility access (most common)

1. Sidebar → **Administration → Teams**
2. Search for `heart360tk_facility_view_patients_<facility_slug>`
3. If the team **exists** → open it, click **Add member**, pick the user, click **Add**
4. If the team **does not exist** → click **New team**, name it exactly per the pattern (`heart360tk_facility_view_patients_<facility_slug>`), save, then add the user
5. Tell the user to log in. Done.

### Step 3 — Give all-facility access (supervisors, admins)

Same as Step 2, but use `heart360tk_facility_view_patients_ALL`.

---

## What the User Sees on Login

```
┌──────────────────────┐
│  User logs in        │
└──────────┬───────────┘
           │
           ▼
  ┌──────────────────────┐
  │ On a facility team?  │
  └──────────┬───────────┘
             │
   ┌─────────┴────────┐
   │ YES              │ NO
   ▼                  ▼
┌───────────────┐   ┌──────────────────────┐
│ Facility      │   │ Global Hypertension  │
│ Secure        │   │ (admin / no-team)    │
│ (their        │   │                      │
│  facility)    │   │                      │
└───────────────┘   └──────────────────────┘
```

- **Admin** → Global Hypertension dashboard
- **Facility user** → Their facility's secure dashboard, facility pre-selected
- **No team** → Global Hypertension dashboard (read-only fallback)

---

## Common Scenarios

- **I want a nurse to see only her facility** → Add her to `heart360tk_facility_view_patients_<her_facility_slug>`
- **I want a regional supervisor to see all facilities** → Add her to `heart360tk_facility_view_patients_ALL`
- **I want someone to see only HTN & DM graphs, not the patient overdue list** → Use `_aggregated_` instead of `_patients_` in the team name
- **A user transferred to a different facility**
  1. Remove them from the old facility team
  2. Add them to the new facility team
  3. Have them log out and log back in (refresh session)
- **Reset a user's password** → Administration → Users → click the user → Edit → change password

---

## Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| User logs in, sees "Access Denied" red banner | They're not on any facility team | Add them to the correct team |
| User added to team, still no access | Facility slug doesn't match, session cached the old state | Rename team using the slug rules above; have user log out and log back in |
| User lands on Global instead of their facility | Not on a facility team or session cache | Check team membership, then log out and back in |

---

## FAQ

- Can a user be on multiple facility teams?
- Do I need both `_patients_` and `_aggregated_` teams?
- How do I make someone an admin?
- What's the default admin login?
- I created a new team but the user still can't see data

---

## Quick Reference Card

```
TEAM NAME PATTERN
─────────────────────────────────────────────────────
heart360tk_facility_view_<TYPE>_<FACILITY_SLUG>
                          │       │
                          │       └─ "phc_garden", "mhow_phc", or "ALL"
                          │
                          └─ "patients"   (full access)
                             "aggregated" (graphs access only)


COMMON TEAMS
─────────────────────────────────────────────────────
Single facility, full     heart360tk_facility_view_patients_<slug>
Single facility, graphs   heart360tk_facility_view_aggregated_<slug>
All facilities, full      heart360tk_facility_view_patients_ALL
All facilities, graphs    heart360tk_facility_view_aggregated_ALL


WORKFLOW
─────────────────────────────────────────────────────
1. Administration → Users → New user
2. Administration → Teams → find/create team
3. Open team → Add member → pick user
4. Tell user to log in
```
