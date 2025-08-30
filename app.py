
import os
import json
import math
import random
import string
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import streamlit as st
import pandas as pd
from dotenv import load_dotenv
import requests


from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from langchain.tools import StructuredTool
from pydantic import BaseModel, Field  # Pydantic v2
from langchain.agents import create_openai_tools_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.memory import ConversationBufferMemory
from langchain.tools.render import render_text_description  # inject tool text
from streamlit_mic_recorder import speech_to_text

try:
    from geopy.geocoders import Nominatim
    GEOCODER = Nominatim(user_agent="communitymate")
except Exception:
    GEOCODER = None


load_dotenv()


DEMO_MODE = os.getenv("DEMO_MODE", "1") == "1"
OUT_DIR = Path("out")
OUT_DIR.mkdir(exist_ok=True)

OSRM_BASE_URL = os.getenv("OSRM_BASE_URL", "https://router.project-osrm.org")

ALLOW_NON_GOV = os.getenv("ALLOW_NON_GOV", "0") == "1"
DEFAULT_ALLOWED_KEYWORDS = [
    "government", "gov", "council", "services australia", "service nsw", "service sa", "service vic",
    "medicare", "centrelink", "child support", "food relief", "foodbank", "community", "community centre",
    "library", "legal aid", "housing", "tenancy", "mental health", "family", "youth", "seniors",
    "aged care", "disability", "ndis", "multicultural", "migrant", "refugee", "homeless",
    "domestic violence", "dv", "women", "men's shed", "aboriginal", "first nations",
    "financial counselling", "emergency relief", "volunteer", "charity", "ngo", "not-for-profit",
    "neighbourhood", "community hub"
]
ALLOWED_SERVICE_KEYWORDS = [
    s.strip().lower() for s in os.getenv("ALLOWED_SERVICE_KEYWORDS", ",".join(DEFAULT_ALLOWED_KEYWORDS)).split(",")
    if s.strip()
]


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    if None in [lat1, lon1, lat2, lon2]:
        return float("inf")
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def geocode_location(q: str) -> Optional[Dict[str, float]]:
    if not q:
        return None
    try:
        if GEOCODER is None:
            return None
        loc = GEOCODER.geocode(q + ", Australia", timeout=10)
        if loc:
            return {"lat": loc.latitude, "lon": loc.longitude}
    except Exception:
        return None
    return None


def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in cols:
            return cols[c.lower()]
    for c in df.columns:
        lc = c.lower()
        for cand in candidates:
            if cand.lower() in lc:
                return c
    return None


def _gen_ref(prefix="CM"):
    return f"{prefix}-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


def _safe_int(x: Any) -> Optional[int]:
    try:
        return int(str(x).strip())
    except Exception:
        return None


def resolve_service_id(service_ref: Optional[str]) -> Optional[str]:
    """
    Resolve selection like "1", "first", or by name, into a service rid (e.g., "svc_0") using last results or global meta.
    """
    if not service_ref:
        return None
    # direct rid like "svc_0"
    if INDEX and service_ref in INDEX.meta:
        return service_ref
    ref = str(service_ref).strip().lower()
    # numeric selection (1-based)
    idx = _safe_int(ref)
    if idx is not None and 1 <= idx <= len(STATE.last_results):
        return STATE.last_results[idx - 1]["id"]
    # common words
    if ref in {"first", "top", "1st"} and STATE.last_results:
        return STATE.last_results[0]["id"]
    # match by name from last_results
    for r in STATE.last_results:
        if ref in r["name"].lower():
            return r["id"]
    # fallback: scan all known services by name
    if INDEX:
        for rid, meta in INDEX.meta.items():
            if ref in (meta.get("name") or "").lower():
                return rid
    return None


def _is_near_me_text(s: Optional[str]) -> bool:
    if not s:
        return True
    ref = s.strip().lower()
    return ref in {"", "near me", "nearby", "home", "my place", "my address"}


def _resolve_origin(location_text: Optional[str]) -> Tuple[Optional[Dict[str, float]], Optional[str]]:
    """
    Returns (origin_coords, origin_label).
    Prefers saved home if user says 'near me' or empty, otherwise geocodes the provided text.
    """
    # Use saved home if asked for 'near me' or no location provided
    if _is_near_me_text(location_text):
        home_lat = STATE.profile.get("home_lat")
        home_lon = STATE.profile.get("home_lon")
        home_addr = STATE.profile.get("home_address")
        if home_lat is not None and home_lon is not None:
            return {"lat": home_lat, "lon": home_lon}, home_addr or "Home"
    # fall through to geocode the input (if any)
    if location_text:
        coords = geocode_location(location_text)
        if coords:
            return coords, location_text
    return None, None


def _gmaps_dir_link(origin_lat, origin_lon, dest_lat, dest_lon, mode="driving"):
    o = f"{origin_lat},{origin_lon}" if origin_lat is not None and origin_lon is not None else ""
    d = f"{dest_lat},{dest_lon}" if dest_lat is not None and dest_lon is not None else ""
    return f"https://www.google.com/maps/dir/?api=1&origin={quote_plus(o)}&destination={quote_plus(d)}&travelmode={quote_plus(mode)}"


def _apple_maps_dir_link(origin_lat, origin_lon, dest_lat, dest_lon, mode="driving"):
    # dirflg: d=driving, w=walking, r=transit
    flg = {"driving": "d", "walking": "w", "transit": "r"}.get(mode, "d")
    params = []
    if origin_lat is not None and origin_lon is not None:
        params.append(f"saddr={origin_lat},{origin_lon}")
    params.append(f"daddr={dest_lat},{dest_lon}")
    params.append(f"dirflg={flg}")
    return "https://maps.apple.com/?" + "&".join(params)


def _osrm_route_duration_km(profile: str, o_lat: float, o_lon: float, d_lat: float, d_lon: float, timeout=6) -> Optional[Tuple[float, float]]:
    """
    Returns (duration_minutes, distance_km) using OSRM, or None if failed.
    """
    try:
        url = f"{OSRM_BASE_URL}/route/v1/{profile}/{o_lon},{o_lat};{d_lon},{d_lat}?overview=false"
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        routes = data.get("routes") or []
        if not routes:
            return None
        duration_s = routes[0].get("duration")  # seconds
        distance_m = routes[0].get("distance")  # metres
        if duration_s is None or distance_m is None:
            return None
        return (duration_s / 60.0, distance_m / 1000.0)
    except Exception:
        return None


def _estimate_transit_minutes(distance_km: float, driving_minutes: Optional[float], walking_minutes: Optional[float]) -> float:
    """
    Heuristic estimate for public transport travel time in minutes (AU metro-ish).
    """
    if distance_km <= 1.2:
        base = (walking_minutes or (distance_km / 4.5 * 60)) + 5
    elif distance_km <= 5:
        base_drive = driving_minutes if driving_minutes is not None else (distance_km / 33 * 60)
        base = base_drive * 2.0 + 6
    else:
        base_drive = driving_minutes if driving_minutes is not None else (distance_km / 35 * 60)
        base = base_drive * 1.9 + 8

    walk = walking_minutes if walking_minutes is not None else (distance_km / 4.5 * 60)
    base = max(base, walk * 0.6 + 8)
    base = min(base, walk * 1.2 + 40)
    return max(5, round(base))


def travel_estimates(origin: Optional[Dict[str, float]], dest: Optional[Dict[str, float]]) -> Dict[str, Any]:
    """
    Returns dict with driving/walking/transit estimates and map links.
    """
    out = {
        "driving_minutes": None,
        "driving_km": None,
        "walking_minutes": None,
        "transit_minutes_est": None,
        "source": "heuristic",
        "map_links": {}
    }
    if not origin or not dest:
        return out

    o_lat, o_lon = origin["lat"], origin["lon"]
    d_lat, d_lon = dest["lat"], dest["lon"]

    # Distance baseline
    straight_km = haversine_km(o_lat, o_lon, d_lat, d_lon)

    # Try OSRM for driving and walking
    drive = _osrm_route_duration_km("driving", o_lat, o_lon, d_lat, d_lon)
    walk = _osrm_route_duration_km("walking", o_lat, o_lon, d_lat, d_lon)

    if drive:
        out["driving_minutes"], out["driving_km"] = round(drive[0]), round(drive[1], 1)
        out["source"] = "osrm"
    else:
        # City driving estimate if OSRM unavailable
        est_drive_min = (straight_km / 33.0) * 60.0  # 33 km/h avg
        out["driving_minutes"] = round(est_drive_min)
        out["driving_km"] = round(straight_km * 1.25, 1)  # road distance a bit longer than straight line

    if walk:
        out["walking_minutes"] = round(walk[0])
    else:
        out["walking_minutes"] = round((straight_km / 4.5) * 60.0)  # 4.5 km/h walking speed

    out["transit_minutes_est"] = _estimate_transit_minutes(
        distance_km=out["driving_km"] if out["driving_km"] else straight_km,
        driving_minutes=out["driving_minutes"],
        walking_minutes=out["walking_minutes"]
    )

    # Map links
    out["map_links"] = {
        "google": {
            "driving": _gmaps_dir_link(o_lat, o_lon, d_lat, d_lon, "driving"),
            "transit": _gmaps_dir_link(o_lat, o_lon, d_lat, d_lon, "transit"),
            "walking": _gmaps_dir_link(o_lat, o_lon, d_lat, d_lon, "walking"),
        },
        "apple": {
            "driving": _apple_maps_dir_link(o_lat, o_lon, d_lat, d_lon, "driving"),
            "transit": _apple_maps_dir_link(o_lat, o_lon, d_lat, d_lon, "transit"),
            "walking": _apple_maps_dir_link(o_lat, o_lon, d_lat, d_lon, "walking"),
        }
    }
    return out


def seed_sample_dataset(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = [
        {
            "name": "Services Australia — Parramatta Service Centre",
            "address": "17-21 Macquarie St, Parramatta NSW 2150",
            "latitude": -33.817083, "longitude": 151.005496,
            "phone": "132 307",
            "website": "https://www.servicesaustralia.gov.au/",
            "description": "Medicare, Centrelink and Child Support services.",
            "category": "Government Service Centre",
            "hours": "Mon–Fri 8:30am–4:30pm"
        },
        {
            "name": "Foodbank NSW & ACT — Glendenning",
            "address": "50 Owen St, Glendenning NSW 2761",
            "latitude": -33.778108, "longitude": 150.853033,
            "phone": "(02) 9756 3099",
            "website": "https://www.foodbank.org.au/nswact/",
            "description": "Food relief and community support.",
            "category": "Food Relief",
            "hours": "Mon–Fri 9:00am–3:00pm"
        },
        {
            "name": "City of Parramatta — Community Centre",
            "address": "10 Darcy St, Parramatta NSW 2150",
            "latitude": -33.817800, "longitude": 151.004900,
            "phone": "(02) 9806 5050",
            "website": "https://www.cityofparramatta.nsw.gov.au/",
            "description": "Local community programs and referrals.",
            "category": "Community Centre",
            "hours": "Mon–Fri 9:00am–5:00pm"
        },
        {
            "name": "Legal Aid NSW — Parramatta",
            "address": "2 George St, Parramatta NSW 2150",
            "latitude": -33.813200, "longitude": 151.003500,
            "phone": "1300 888 529",
            "website": "https://www.legalaid.nsw.gov.au/",
            "description": "Legal help and advice. Civil, family and criminal law.",
            "category": "Legal Aid",
            "hours": "Mon–Fri 9:00am–5:00pm"
        },
        {
            "name": "Service NSW — Parramatta Service Centre",
            "address": "27-31 Argyle St, Parramatta NSW 2150",
            "latitude": -33.817900, "longitude": 151.006300,
            "phone": "13 77 88",
            "website": "https://www.service.nsw.gov.au/",
            "description": "Licences, registrations, NSW Government services.",
            "category": "Government Service Centre",
            "hours": "Mon–Fri 9:00am–5:00pm, Sat 9:00am–3:00pm"
        }
    ]
    pd.DataFrame(data).to_csv(path, index=False)

# -----------------------------------------------------------------------------
# Gov/community service filter
# -----------------------------------------------------------------------------
def is_gov_or_community(meta: Dict[str, Any]) -> bool:
    if ALLOW_NON_GOV:
        return True
    # Check website domain
    web = (meta.get("website") or "").lower()
    if ".gov.au" in web or ".org.au" in web:
        return True
    # Keywords in category/name/description
    text = " ".join([
        str(meta.get("category") or ""),
        str(meta.get("name") or ""),
        str(meta.get("description") or "")
    ]).lower()
    for kw in ALLOWED_SERVICE_KEYWORDS:
        if kw in text:
            return True
    return False


class ServiceIndex:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.df = self._load_csv(csv_path)
        self.docs, self.meta = self._to_docs(self.df)
        self.embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        self.vs = FAISS.from_documents(self.docs, self.embeddings)

    def _load_csv(self, path: str) -> pd.DataFrame:
        df = pd.read_csv(path)
        return df

    def _to_docs(self, df: pd.DataFrame):
        name_col = pick_col(df, ["name", "service_name", "office_name", "title"])
        desc_col = pick_col(df, ["description", "details", "about", "notes"])
        addr_col = pick_col(df, ["address", "street_address", "full_address", "site_address", "location"])
        lat_col = pick_col(df, ["lat", "latitude", "y"])
        lon_col = pick_col(df, ["lon", "lng", "longitude", "x"])
        phone_col = pick_col(df, ["phone", "contact_number", "telephone"])
        email_col = pick_col(df, ["email", "contact_email"])
        web_col = pick_col(df, ["website", "url", "booking_url", "link"])
        hours_col = pick_col(df, ["hours", "opening_hours", "open_hours"])
        cat_col = pick_col(df, ["category", "categories", "service_type", "services", "tags"])
        lga_col = pick_col(df, ["lga", "local_government_area", "council"])

        docs = []
        meta = {}
        for i, row in df.iterrows():
            rid = f"svc_{i}"
            name = str(row.get(name_col, "")).strip() if name_col else f"Service {i}"
            desc = str(row.get(desc_col, "")).strip() if desc_col else ""
            addr = str(row.get(addr_col, "")).strip() if addr_col else ""
            phone = str(row.get(phone_col, "")).strip() if phone_col else ""
            email = str(row.get(email_col, "")).strip() if email_col else ""
            web = str(row.get(web_col, "")).strip() if web_col else ""
            hours = str(row.get(hours_col, "")).strip() if hours_col else ""
            cat = str(row.get(cat_col, "")).strip() if cat_col else ""
            lga = str(row.get(lga_col, "")).strip() if lga_col else ""

            try:
                lat = float(row.get(lat_col)) if lat_col and not pd.isna(row.get(lat_col)) else None
                lon = float(row.get(lon_col)) if lon_col and not pd.isna(row.get(lon_col)) else None
            except Exception:
                lat, lon = None, None

            text = f"{name}. Category: {cat}. Address: {addr}. LGA: {lga}. Hours: {hours}. Details: {desc}. Contact: {phone} {email} {web}"
            d = Document(page_content=text, metadata={"rid": rid})
            docs.append(d)

            meta[rid] = {
                "id": rid,
                "name": name,
                "category": cat,
                "address": addr,
                "lga": lga,
                "hours": hours,
                "description": desc,
                "phone": phone,
                "email": email,
                "website": web,
                "lat": lat,
                "lon": lon,
            }
        return docs, meta

    def search(self, query: str, where: Optional[str], radius_km: int = 20, top_k: int = 5) -> Dict[str, Any]:
        hits = self.vs.similarity_search(query, k=50)

        origin_coords, origin_label = _resolve_origin(where)
        results_all = []
        for h in hits:
            m = self.meta[h.metadata["rid"]]
            if not is_gov_or_community(m):
                continue  # strict scope: only government/community
            dist = None
            if origin_coords and m.get("lat") is not None and m.get("lon") is not None:
                dist = haversine_km(origin_coords["lat"], origin_coords["lon"], m["lat"], m["lon"])
            m2 = dict(m)
            m2["distance_km"] = round(dist, 2) if dist is not None else None
            results_all.append(m2)

        # Sort and filter by straight-line distance when origin known; else leave semantic order
        if origin_coords:
            results_all = sorted(results_all, key=lambda x: x["distance_km"] if x["distance_km"] is not None else 9e9)
            if radius_km:
                filtered = [r for r in results_all if r["distance_km"] is not None and r["distance_km"] <= radius_km]
                results_all = filtered or results_all  # fallback if no geo data

        # Only compute travel for the final top_k to save API calls
        selected = results_all[:top_k]

        # Add travel estimates and map links
        if origin_coords:
            for r in selected:
                dest = {"lat": r.get("lat"), "lon": r.get("lon")} if r.get("lat") is not None and r.get("lon") is not None else None
                travel = travel_estimates(origin_coords, dest)
                r.update({
                    "travel": travel,
                    "map_links": travel.get("map_links", {}),
                })

        # Sort again by transport preference if origin known and travel available
        pref = (STATE.profile.get("transport_preference") or "any").lower()
        if origin_coords and selected:
            def pref_key(item):
                t = item.get("travel") or {}
                if pref == "walk":
                    return t.get("walking_minutes") or 1e9
                if pref == "transit":
                    return t.get("transit_minutes_est") or 1e9
                if pref == "car" or pref == "driving":
                    return t.get("driving_minutes") or 1e9
                # default any: keep earlier ordering (distance)
                return item.get("distance_km") or 1e9
            selected = sorted(selected, key=pref_key)

        return {
            "origin": {"coords": origin_coords, "label": origin_label, "raw": where},
            "results": selected
        }


class SessionState:
    def __init__(self):
        self.profile = {
            "name": None,
            "language": "en",
            "contact_channel": "sms",  # sms | email | none
            "contact_value": None,
            "consent": False,
            "access_needs": [],  # e.g., ["large_text", "easy_read"]
            "home_address": None,
            "home_lat": None,
            "home_lon": None,
            "transport_preference": "any",  # car | transit | walk | any
        }
        self.bookings: List[Dict[str, Any]] = []
        self.reminders: List[Dict[str, Any]] = []
        self.handoffs: List[Dict[str, Any]] = []
        self.last_results: List[Dict[str, Any]] = []  # store last search results (flattened list)

    def save(self, path=str(OUT_DIR / "session_state.json")):
        with open(path, "w") as f:
            json.dump({
                "profile": self.profile,
                "bookings": self.bookings,
                "reminders": self.reminders,
                "handoffs": self.handoffs,
                "last_results": self.last_results,
            }, f, indent=2)

# Singletons (rebound to st.session_state instances in app init)
STATE: SessionState = None  # type: ignore
INDEX: Optional[ServiceIndex] = None


class FindServicesArgs(BaseModel):
    query: str = Field(..., description="User's need, e.g., 'food relief near me', 'Medicare centre', 'housing support'.")
    location: str = Field("", description="Address, suburb or postcode in Australia. If empty or 'near me', uses saved home if available.")
    radius_km: int = Field(20, description="Radius in km for results, default 20.")
    top_k: int = Field(5, description="How many results to return, default 5.")


def find_services_tool(query: str, location: str = "", radius_km: int = 20, top_k: int = 5) -> str:
    if INDEX is None:
        return json.dumps({"error": "Index not initialised"})
    payload = INDEX.search(query=query, where=location, radius_km=radius_km, top_k=top_k)
    results = payload.get("results", [])
    # Save last results for numeric selection
    STATE.last_results = results
    STATE.save()
    return json.dumps(payload)


find_services = StructuredTool.from_function(
    name="find_services",
    description="Find relevant nearby Australian government/community services. Includes travel estimates and map links when origin is known (home or provided location).",
    func=find_services_tool,
    args_schema=FindServicesArgs,
)

class RecordConsentArgs(BaseModel):
    granted: bool = Field(..., description="Whether user grants consent to share minimal info to book or submit forms.")
    scope: str = Field(..., description="What the consent covers, e.g., 'booking', 'form', 'reminder'.")


def record_consent_tool(granted: bool, scope: str) -> str:
    STATE.profile["consent"] = bool(granted)
    STATE.save()
    return json.dumps({"consent": STATE.profile["consent"], "scope": scope})


record_consent = StructuredTool.from_function(
    name="record_consent",
    description="Record user consent before taking actions requiring sharing personal info (booking/form/reminder).",
    func=record_consent_tool,
    args_schema=RecordConsentArgs,
)

class BookServiceArgs(BaseModel):
    service_id: str = Field(..., description="The service identifier or selection like '1' for first result.")
    user_name: Optional[str] = Field(None, description="User's full name.")
    contact_channel: str = Field("sms", description="sms or email")
    contact_value: Optional[str] = Field(None, description="Phone for SMS or email address.")
    time_preference: Optional[str] = Field(None, description="Preferred time window, e.g., 'tomorrow 9am'.")


def book_service_tool(service_id: str, user_name: Optional[str] = None, contact_channel: str = "sms",
                      contact_value: Optional[str] = None, time_preference: Optional[str] = None) -> str:
    if not STATE.profile.get("consent"):
        return json.dumps({"error": "CONSENT_REQUIRED", "message": "Consent needed before booking."})

    rid = resolve_service_id(service_id)
    if not rid or INDEX is None or rid not in INDEX.meta:
        return json.dumps({"error": "SERVICE_NOT_FOUND", "message": "Please choose a listed option number (e.g., 1) or name."})

    svc = INDEX.meta[rid]
    if not is_gov_or_community(svc):
        return json.dumps({"error": "OUT_OF_SCOPE", "message": "Bookings restricted to government/community services."})

    ref = _gen_ref("BOOK")
    when = time_preference or "next available"
    booking = {
        "booking_ref": ref,
        "demo": DEMO_MODE,
        "service_id": rid,
        "service_name": svc["name"],
        "when": when,
        "channel": contact_channel,
        "contact": contact_value,
        "user_name": user_name,
        "service_address": svc.get("address", ""),
        "service_phone": svc.get("phone", ""),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "status": "requested"  # simulated, not a real booking
    }
    STATE.bookings.append(booking)
    STATE.save()

    receipt_path = OUT_DIR / f"{ref}_receipt.json"
    with open(receipt_path, "w") as f:
        json.dump(booking, f, indent=2)

    confirmation_text = f"Demo booking created with {svc['name']} for {when}. Ref: {ref}. We’ll contact via {contact_channel}."
    return json.dumps({"ok": True, "booking": booking, "receipt_path": str(receipt_path), "message": confirmation_text})


book_service = StructuredTool.from_function(
    name="book_service",
    description="Simulate booking with a selected service (stores a local booking 'request'). Accepts '1' for first result. Requires consent.",
    func=book_service_tool,
    args_schema=BookServiceArgs,
)

class SetReminderArgs(BaseModel):
    service_id: str = Field(..., description="Service id or selection like '1'.")
    when: str = Field(..., description="Time for reminder, e.g., '2025-09-15 09:00' or '2025-09-15'.")
    channel: str = Field("sms", description="sms or email.")


def _parse_when(when_str: str) -> Optional[datetime]:
    try:
        return datetime.strptime(when_str, "%Y-%m-%d %H:%M")
    except Exception:
        pass
    try:
        d = datetime.strptime(when_str, "%Y-%m-%d")
        return d.replace(hour=9, minute=0)
    except Exception:
        return None


def _write_ics(summary: str, description: str, start_dt: datetime, duration_minutes=30,
               location: str = "", path: Path = OUT_DIR / "reminder.ics") -> Path:
    dtstamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    dtstart = start_dt.strftime("%Y%m%dT%H%M%S")  # naive local time for demo
    dtend = (start_dt + timedelta(minutes=duration_minutes)).strftime("%Y%m%dT%H%M%S")
    uid = _gen_ref("ICS")

    ics = "\n".join([
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//CommunityMate//EN",
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{dtstamp}",
        f"DTSTART:{dtstart}",
        f"DTEND:{dtend}",
        f"SUMMARY:{summary}",
        f"DESCRIPTION:{description}",
        f"LOCATION:{location}",
        "END:VEVENT",
        "END:VCALENDAR",
        ""
    ])
    with open(path, "w") as f:
        f.write(ics)
    return path


def set_reminder_tool(service_id: str, when: str, channel: str = "sms") -> str:
    if not STATE.profile.get("consent"):
        return json.dumps({"error": "CONSENT_REQUIRED", "message": "Consent needed before setting reminder."})
    rid = resolve_service_id(service_id)
    svc = INDEX.meta.get(rid) if INDEX and rid else None
    if svc and not is_gov_or_community(svc):
        return json.dumps({"error": "OUT_OF_SCOPE", "message": "Reminders restricted to government/community services."})

    reminder_ref = _gen_ref("REM")
    start_dt = _parse_when(when) or (datetime.utcnow() + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
    ics_path = _write_ics(
        summary=f"Visit: {svc['name'] if svc else 'Service'}",
        description="This is a demo reminder. No real booking was made.",
        start_dt=start_dt,
        duration_minutes=30,
        location=svc["address"] if svc else "",
        path=OUT_DIR / f"{reminder_ref}.ics"
    )
    reminder = {
        "reminder_ref": reminder_ref,
        "service_id": rid or service_id,
        "service_name": svc["name"] if svc else "",
        "when": when,
        "channel": channel,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "ics_path": str(ics_path),
        "demo": DEMO_MODE
    }
    STATE.reminders.append(reminder)
    STATE.save()
    return json.dumps({"ok": True, "reminder": reminder, "message": f"Reminder set. File: {ics_path}"})


set_reminder = StructuredTool.from_function(
    name="set_reminder",
    description="Set a reminder for an appointment or action. Accepts '1' for first result. Creates a local .ics file. Requires consent.",
    func=set_reminder_tool,
    args_schema=SetReminderArgs,
)

class FillFormArgs(BaseModel):
    service_id: str = Field(..., description="Service id or selection like '1'.")
    answers_json: str = Field(..., description="JSON string of form answers.")


def fill_form_tool(service_id: str, answers_json: str) -> str:
    if not STATE.profile.get("consent"):
        return json.dumps({"error": "CONSENT_REQUIRED", "message": "Consent needed before submitting form."})
    rid = resolve_service_id(service_id)
    svc = INDEX.meta.get(rid) if INDEX and rid else None
    if svc and not is_gov_or_community(svc):
        return json.dumps({"error": "OUT_OF_SCOPE", "message": "Forms restricted to government/community services."})
    try:
        answers = json.loads(answers_json)
    except Exception:
        answers = {"raw": answers_json}
    record = {
        "service_id": rid or service_id,
        "answers": answers,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "status": "submitted"  # simulated
    }
    STATE.bookings.append({"form_submission": record})
    STATE.save()

    ref = _gen_ref("FORM")
    receipt_path = OUT_DIR / f"{ref}_form.json"
    with open(receipt_path, "w") as f:
        json.dump(record, f, indent=2)

    return json.dumps({"ok": True, "form_submission": record, "receipt_path": str(receipt_path), "message": "Demo form submitted."})


fill_form = StructuredTool.from_function(
    name="fill_form",
    description="Submit a simple form payload to the selected service (simulated). Accepts '1' for first result. Requires consent.",
    func=fill_form_tool,
    args_schema=FillFormArgs,
)

class EscalateArgs(BaseModel):
    service_id: Optional[str] = Field(None, description="Optional service id if known or selection like '1'.")
    issue: str = Field(..., description="Short text to share with a volunteer/human helper.")


def escalate_tool(service_id: Optional[str], issue: str) -> str:
    rid = resolve_service_id(service_id) if service_id else None
    note = {
        "service_id": rid or service_id,
        "issue": issue,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "status": "queued",
        "demo": DEMO_MODE
    }
    STATE.handoffs.append(note)
    STATE.save()

    ref = _gen_ref("HELP")
    path = OUT_DIR / f"{ref}_handoff.json"
    with open(path, "w") as f:
        json.dump(note, f, indent=2)

    return json.dumps({"ok": True, "handoff": note, "receipt_path": str(path), "message": "A volunteer will be notified (simulated)."})


escalate_to_human = StructuredTool.from_function(
    name="escalate_to_human",
    description="Create a handover note to a volunteer for follow-up (simulated).",
    func=escalate_tool,
    args_schema=EscalateArgs,
)

# New: Update profile tool to save home, contact details, language, etc.
class UpdateProfileArgs(BaseModel):
    name: Optional[str] = Field(None, description="User's name.")
    language: Optional[str] = Field(None, description="Preferred language (e.g., 'en', 'es').")
    contact_channel: Optional[str] = Field(None, description="sms | email | none")
    contact_value: Optional[str] = Field(None, description="Phone or email value if provided.")
    home_address: Optional[str] = Field(None, description="Home address (used for 'near me' searches).")
    transport_preference: Optional[str] = Field(None, description="car | transit | walk | any")
    access_needs: Optional[List[str]] = Field(None, description="List of access needs like 'easy_read', 'large_text'.")


def update_profile_tool(name: Optional[str] = None, language: Optional[str] = None, contact_channel: Optional[str] = None,
                        contact_value: Optional[str] = None, home_address: Optional[str] = None,
                        transport_preference: Optional[str] = None, access_needs: Optional[List[str]] = None) -> str:
    prof = STATE.profile
    if name is not None:
        prof["name"] = name
    if language is not None:
        prof["language"] = language
    if contact_channel is not None:
        prof["contact_channel"] = contact_channel
    if contact_value is not None:
        prof["contact_value"] = contact_value
    if transport_preference is not None:
        prof["transport_preference"] = transport_preference
    if access_needs is not None:
        prof["access_needs"] = access_needs

    if home_address is not None:
        prof["home_address"] = home_address
        coords = geocode_location(home_address)
        if coords:
            prof["home_lat"] = coords["lat"]
            prof["home_lon"] = coords["lon"]
        else:
            prof["home_lat"] = None
            prof["home_lon"] = None

    STATE.save()
    # Return a privacy-friendly profile view
    safe_prof = {k: v for k, v in prof.items() if k not in {"contact_value"}}
    return json.dumps({"ok": True, "profile": safe_prof})


update_profile = StructuredTool.from_function(
    name="update_profile",
    description="Save user preferences like name, language, contact, home address (for 'near me'), transport preference, and access needs.",
    func=update_profile_tool,
    args_schema=UpdateProfileArgs,
)

TOOLS = [find_services, record_consent, book_service, set_reminder, fill_form, escalate_to_human, update_profile]

# -----------------------------------------------------------------------------
# Agent
# -----------------------------------------------------------------------------
SYSTEM_PROMPT = """You are CommunityMate, an inclusive Australian community assistant focused ONLY on government and community services.

Scope and boundaries:

You only help with discovering, accessing, and engaging with Australian government or local community/charity services.
If a user asks for something unrelated, explain your scope briefly and offer to connect them with a relevant service instead.
Do not give clinical, legal, or financial advice. Refer to appropriate services (e.g., Legal Aid, health services).
If the user indicates immediate danger or crisis: advise to call 000 (emergency). For mental health crisis suggest Lifeline 13 11 14, Beyond Blue 1300 22 4636. For family/domestic violence suggest 1800RESPECT (1800 737 732). Include state equivalents only as examples, not definitive lists.
Goals:

Understand user needs in plain language, any language.
Ask up to 1–3 short, friendly questions if key info is missing (where from, how you'd like to get there: car/transit/walk, and when).
Be encouraging and easy to read. Offer simple steps: choose an option, get directions, set a reminder or booking (demo).
Proactively find nearby, relevant government/community services via the find_services tool.
Offer to book, fill forms, and set reminders, but ONLY after getting explicit consent via record_consent.
Be inclusive: short sentences, clear steps, respect your language. Offer easy-read on request or when access needs are present.
If user has barriers (language, disability), simplify and offer handover to a volunteer.
Never invent facts. If unsure, say so.
Results style:

Start with a friendly sentence and a quick plan.
Then list 2–5 options with compact bullets: name, what they help with, hours, phone, website.
Include distance/travel time from the user's origin when available: driving and public transport (transit time may be an estimate).
Provide map links for driving and transit so the user can open directions easily.
If the user asks for fewer details, keep it short. If they want more, expand.
If 'near me' or no location is provided, use saved home (if available). Otherwise, ask for a suburb or postcode.
Safety & privacy:

Ask for consent before book_service, fill_form, or set_reminder.
Minimise data. Only store what is needed. Confirm what is stored and why.
If tools return CONSENT_REQUIRED, ask the user for permission and then call record_consent if they agree.
Behaviour:

Default to local Australian context.
If the user's message implies a language (e.g., Spanish), reply in that language.
When useful, invite the user to save their home address for quicker help next time (use update_profile).
Use tools when helpful. Think step-by-step.
"""

def build_agent():
    model_name = os.getenv("LLM_MODEL", "gpt-4o-mini")
    llm = ChatOpenAI(model=model_name, temperature=0.2)

    rendered_tools = render_text_description(TOOLS)
    tool_names = ", ".join([t.name for t in TOOLS])

    prompt = ChatPromptTemplate.from_messages([
        ("system",
         SYSTEM_PROMPT
         + "\n\nAvailable tools:\n{tools}\n"
         + "Tool names: {tool_names}\n"
         + "Use tools when helpful. Think step-by-step."),
        MessagesPlaceholder(variable_name="chat_history"),
        ("human", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ]).partial(tools=rendered_tools, tool_names=tool_names)

    agent = create_openai_tools_agent(llm=llm, tools=TOOLS, prompt=prompt)
    # Memory for conversation
    memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
    executor = AgentExecutor(agent=agent, tools=TOOLS, memory=memory, verbose=False)
    return executor

# -----------------------------------------------------------------------------
# Streamlit App
# -----------------------------------------------------------------------------
st.set_page_config(page_title="CommunityMate (DEMO)", page_icon="🇦🇺", layout="centered")
# Style for a floating mic button near the chat input
st.markdown("""
<style>
  .cm-mic-wrap {
    position: fixed;
    right: 16px;
    bottom: 86px; /* tweak to align perfectly with your chat input */
    z-index: 1000;
  }
  .cm-mic-wrap button {
    width: 44px !important;
    height: 44px !important;
    padding: 0 !important;
    border-radius: 999px !important;
    font-size: 20px !important;
    line-height: 44px !important;
  }
  @media (max-width: 640px){
    .cm-mic-wrap { right: 12px; bottom: 92px; }
    .cm-mic-wrap button { width: 40px !important; height: 40px !important; font-size: 18px !important; }
  }
</style>
""", unsafe_allow_html=True)
with st.sidebar:
    # Logo
    st.image(str(Path(__file__).parent / "logo.jpeg"), use_container_width=True)

    # Short description
    st.markdown(
        "### CommunityMate\n"
        "Australian government & community services assistant (Hackathon Prototype)"
    )

    # Quick info bullets
    st.markdown(
        """
**Scope:** Govt & community/charity services only  
**Demo:** No real bookings. Demo outputs saved locally.  
**Travel:** Car/walk via OSRM; public transport estimated  
**Privacy:** Minimal local storage; consent required  
**Note:** This is a demo — explore features and try different prompts!
        """
    )

    # Realistic suggested prompts
    st.markdown("**Try these prompts:**")
    st.markdown(
        """
- I need food relief for my family in Parramatta  
- How can I get help for my son’s school fees?  
- Nearest Medicare office near me  
- Legal aid or advice for a rental dispute  
- Support services for mental health  
- How to apply for childcare assistance  
- Save my home at 10 Darcy St, Parramatta
        """
    )

    # Reset chat button
    if st.button("Reset chat"):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()

def init_components():
    global STATE, INDEX
    if "cm_state" not in st.session_state:
        st.session_state.cm_state = SessionState()
    if "cm_index" not in st.session_state:
        csv_path = os.environ.get("SERVICES_CSV_PATH", "data/service_centres.csv")
        if not os.path.exists(csv_path):
            seed_sample_dataset(csv_path)
        st.session_state.cm_index = ServiceIndex(csv_path)
    if "cm_agent" not in st.session_state:
        st.session_state.cm_agent = build_agent()
    if "messages" not in st.session_state:
        st.session_state.messages = []
        # Initial friendly note
        st.session_state.messages.append({
            "role": "assistant",
            "content": "Hi! I can help you find Australian government or community services near you. For example: 'I need food relief near Parramatta' or 'Where is the nearest Medicare office?'."
        })

    STATE = st.session_state.cm_state
    INDEX = st.session_state.cm_index

init_components()

st.title("🇦🇺 CommunityMate (Demo)")
st.caption("Strictly for Australian government & community services. Travel times use OSRM/public estimates. Bookings/forms/reminders are simulated; receipts saved to ./out")

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])


mic_placeholder = st.empty()
with mic_placeholder.container():
    st.markdown('<div class="cm-mic-wrap">', unsafe_allow_html=True)
    spoken_text = speech_to_text(
        language="en-AU",
        start_prompt="🎤",  # small round mic
        stop_prompt="⏹",
        just_once=True,
        use_container_width=False,
        key="stt_mic_overlay",
    )
    st.markdown('</div>', unsafe_allow_html=True)

typed_text = st.chat_input("How can I help today?")


user_input = spoken_text or typed_text


if user_input:
    if spoken_text and st.session_state.get("last_input_sent") == user_input:
        user_input = None
    else:
        st.session_state["last_input_sent"] = user_input

if user_input:
   
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

   
    init_components()

    try:
        out = st.session_state.cm_agent.invoke({"input": user_input})
        assistant_text = out.get("output", "").strip()
        if not assistant_text:
            assistant_text = "Sorry, I couldn't generate a response."
    except Exception as e:
        assistant_text = f"Error: {e}"

  
    st.session_state.messages.append({"role": "assistant", "content": assistant_text})
    with st.chat_message("assistant"):
        st.markdown(assistant_text)


st.divider()
st.caption("Tips: You can say 'near me' if you save your home address via the chat. I’ll ask for consent before any booking, form, or reminder. If you're in immediate danger, call 000.")