from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


@dataclass
class Config:
    seed: int = 7
    days: int = 120
    n_creators: int = 300
    n_listeners: int = 50000
    n_shows_per_creator_mean: float = 1.2
    n_episodes_per_show_mean: float = 18.0


def _utc_now_floor_day() -> datetime:
    now = datetime.now(timezone.utc)
    return datetime(now.year, now.month, now.day, tzinfo=timezone.utc)


def _choice(rng: np.random.Generator, arr: List[str], size: int, p: List[float] | None = None) -> np.ndarray:
    return rng.choice(np.array(arr), size=size, replace=True, p=p)


def generate_dimensions(cfg: Config) -> Dict[str, pd.DataFrame]:
    rng = np.random.default_rng(cfg.seed)

    countries = ["US", "CA", "GB", "IN", "BR", "DE", "MX", "FR", "AU"]
    categories = ["Business", "Comedy", "Education", "Health", "News", "Sports", "Tech", "True Crime"]

    # Creators
    creator_ids = np.arange(1, cfg.n_creators + 1)
    creator_country = _choice(rng, countries, cfg.n_creators)
    creator_tier = _choice(rng, ["small", "mid", "large"], cfg.n_creators, p=[0.70, 0.25, 0.05])
    join_day_offset = rng.integers(low=0, high=max(cfg.days, 30), size=cfg.n_creators)

    end_day = _utc_now_floor_day()
    join_ts = [end_day - timedelta(days=int(x)) for x in join_day_offset]

    dim_creator = pd.DataFrame(
        {
            "creator_id": creator_ids,
            "country": creator_country,
            "tier": creator_tier,
            "join_ts": pd.to_datetime(join_ts, utc=True),
        }
    )

    # Shows (creator -> 1..k)
    show_rows = []
    show_id = 1
    for c_id, tier in zip(creator_ids, creator_tier):
        # larger creators tend to have a bit more shows
        lam = cfg.n_shows_per_creator_mean + (0.6 if tier == "large" else 0.2 if tier == "mid" else 0.0)
        k = max(1, int(rng.poisson(lam=lam)))
        for _ in range(k):
            show_rows.append(
                {
                    "show_id": show_id,
                    "creator_id": int(c_id),
                    "category": str(rng.choice(categories)),
                    "language": str(rng.choice(["en", "es", "pt", "de", "fr", "hi"], p=[0.65, 0.10, 0.08, 0.06, 0.06, 0.05])),
                }
            )
            show_id += 1
    dim_show = pd.DataFrame(show_rows)

    # Episodes (show -> many)
    episode_rows = []
    episode_id = 1
    for _, row in dim_show.iterrows():
        lam = cfg.n_episodes_per_show_mean
        k = max(6, int(rng.poisson(lam=lam)))
        # Publish within last cfg.days
        publish_offsets = np.sort(rng.integers(low=0, high=cfg.days, size=k))
        for off in publish_offsets:
            episode_rows.append(
                {
                    "episode_id": episode_id,
                    "show_id": int(row["show_id"]),
                    "creator_id": int(row["creator_id"]),
                    "publish_ts": pd.to_datetime(end_day - timedelta(days=int(off)), utc=True),
                    "duration_s": int(rng.integers(600, 3600)),  # 10m to 60m
                }
            )
            episode_id += 1
    dim_episode = pd.DataFrame(episode_rows)

    # Listeners
    listener_ids = np.arange(1, cfg.n_listeners + 1)
    listener_region = _choice(rng, ["NA", "EU", "LATAM", "APAC"], cfg.n_listeners, p=[0.45, 0.25, 0.15, 0.15])
    signup_offsets = rng.integers(low=0, high=max(cfg.days, 90), size=cfg.n_listeners)
    signup_ts = [end_day - timedelta(days=int(x)) for x in signup_offsets]
    dim_listener = pd.DataFrame(
        {"listener_id": listener_ids, "region": listener_region, "signup_ts": pd.to_datetime(signup_ts, utc=True)}
    )

    return {
        "dim_creator": dim_creator,
        "dim_show": dim_show,
        "dim_episode": dim_episode,
        "dim_listener": dim_listener,
    }


def generate_events(cfg: Config, dims: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      fct_events: listen/follow/comment/share/analytics_view
      fct_ads: ad impressions + revenue (derived from listens)
    """
    rng = np.random.default_rng(cfg.seed + 1)
    end_day = _utc_now_floor_day()

    dim_creator = dims["dim_creator"]
    dim_show = dims["dim_show"]
    dim_episode = dims["dim_episode"]
    dim_listener = dims["dim_listener"]

    # --- Creator-level adoption knobs (analytics + comments) ---
    # analytics adoption: higher for mid/large
    tier_to_adopt = {"small": 0.35, "mid": 0.55, "large": 0.75}
    creator_adopts_analytics = dim_creator.apply(lambda r: rng.random() < tier_to_adopt[r["tier"]], axis=1).astype(bool)

    # comments enabled: not all shows enable it
    show_enable_comments = rng.random(len(dim_show)) < 0.50

    # --- Listening volume by creator tier ---
    tier_base_listens = {"small": 30, "mid": 120, "large": 450}  # per day-ish units
    # build a per-creator daily volume scalar
    creator_volume = dim_creator["tier"].map(tier_base_listens).astype(int).to_numpy()

    # total listen events target
    expected_listens = int(cfg.days * creator_volume.sum() * 0.08)  # scaling factor to keep size manageable

    # sample creators for listens (weighted by volume)
    creator_probs = creator_volume / creator_volume.sum()
    listen_creator_ids = rng.choice(dim_creator["creator_id"].to_numpy(), size=expected_listens, p=creator_probs)

    # for each listen, pick an episode from that creator
    episodes_by_creator = dim_episode.groupby("creator_id")["episode_id"].apply(list).to_dict()
    shows_by_creator = dim_show.groupby("creator_id")["show_id"].apply(list).to_dict()

    listen_episode_ids = np.empty(expected_listens, dtype=int)
    listen_show_ids = np.empty(expected_listens, dtype=int)

    for i, c_id in enumerate(listen_creator_ids):
        ep = rng.choice(episodes_by_creator[int(c_id)])
        listen_episode_ids[i] = int(ep)
        # show is derivable but keep explicit for analytics convenience
        listen_show_ids[i] = int(dim_episode.loc[dim_episode["episode_id"] == ep, "show_id"].iloc[0])

    # assign listeners
    listen_listener_ids = rng.choice(dim_listener["listener_id"].to_numpy(), size=expected_listens, replace=True)

    # timestamp within last cfg.days
    listen_day_offsets = rng.integers(low=0, high=cfg.days, size=expected_listens)
    listen_ts = [end_day - timedelta(days=int(d)) + timedelta(minutes=int(rng.integers(0, 1440))) for d in listen_day_offsets]

    fct_listen = pd.DataFrame(
        {
            "event_ts": pd.to_datetime(listen_ts, utc=True),
            "event_type": "listen",
            "listener_id": listen_listener_ids,
            "creator_id": listen_creator_ids,
            "show_id": listen_show_ids,
            "episode_id": listen_episode_ids,
        }
    )

    # completion rate (proxy) used for ad load / future analysis
    fct_listen["completion_pct"] = np.clip(rng.normal(loc=0.72, scale=0.18, size=len(fct_listen)), 0.05, 1.0)

    # follow events: a fraction of listens convert to follow
    follow_mask = rng.random(len(fct_listen)) < (0.015 + 0.010 * fct_listen["completion_pct"])
    fct_follow = fct_listen.loc[follow_mask, ["event_ts", "listener_id", "creator_id", "show_id", "episode_id"]].copy()
    fct_follow["event_type"] = "follow"

    # comment events: only if show enabled comments + completion reasonably high
    show_comment_enabled_map = dict(zip(dim_show["show_id"].astype(int), show_enable_comments))
    enabled = fct_listen["show_id"].map(show_comment_enabled_map).astype(bool)
    comment_mask = enabled & (fct_listen["completion_pct"] > 0.55) & (rng.random(len(fct_listen)) < 0.006)
    fct_comment = fct_listen.loc[comment_mask, ["event_ts", "listener_id", "creator_id", "show_id", "episode_id"]].copy()
    fct_comment["event_type"] = "comment"

    # share events
    share_mask = (fct_listen["completion_pct"] > 0.60) & (rng.random(len(fct_listen)) < 0.004)
    fct_share = fct_listen.loc[share_mask, ["event_ts", "listener_id", "creator_id", "show_id", "episode_id"]].copy()
    fct_share["event_type"] = "share"

    # creator analytics views (creator-side events, not tied to listener)
    # if creator adopted analytics, they view ~ weekly-ish
    analytics_rows = []
    for c_id, adopted in zip(dim_creator["creator_id"].to_numpy(), creator_adopts_analytics.to_numpy()):
        if not adopted:
            continue
        # number of views over the period
        n_views = max(2, int(rng.poisson(lam=cfg.days / 9)))
        day_offsets = rng.integers(0, cfg.days, size=n_views)
        for d in day_offsets:
            ts = end_day - timedelta(days=int(d)) + timedelta(minutes=int(rng.integers(0, 1440)))
            analytics_rows.append(
                {
                    "event_ts": pd.to_datetime(ts, utc=True),
                    "event_type": "analytics_view",
                    "listener_id": np.nan,
                    "creator_id": int(c_id),
                    "show_id": np.nan,
                    "episode_id": np.nan,
                }
            )
    fct_analytics = pd.DataFrame(analytics_rows)

    # combine events
    fct_events = pd.concat([fct_listen.drop(columns=["completion_pct"]), fct_follow, fct_comment, fct_share, fct_analytics], ignore_index=True)
    fct_events["listener_id"] = fct_events["listener_id"].astype("Int64")

    # --- Ad monetization derived from listens ---
    # impressions proportional to completion + random ad slots
    listens = fct_listen.copy()
    ad_slots = rng.integers(1, 4, size=len(listens))  # 1-3 slots
    fill_rate = np.clip(rng.normal(0.82, 0.08, size=len(listens)), 0.40, 0.98)
    filled = np.floor(ad_slots * fill_rate).astype(int)
    # CPM varies by tier and completion
    tier_map = dim_creator.set_index("creator_id")["tier"].to_dict()
    tier_cpm_base = {"small": 14.0, "mid": 18.0, "large": 22.0}

    cpm = listens["creator_id"].map(lambda x: tier_cpm_base[tier_map[int(x)]]) * (0.85 + 0.35 * listens["completion_pct"])
    cpm = np.clip(cpm + rng.normal(0, 2.0, size=len(cpm)), 5.0, 45.0)

    revenue = filled * (cpm / 1000.0)

    fct_ads = pd.DataFrame(
        {
            "ad_ts": listens["event_ts"].astype("datetime64[ns, UTC]"),
            "creator_id": listens["creator_id"].astype(int),
            "show_id": listens["show_id"].astype(int),
            "episode_id": listens["episode_id"].astype(int),
            "impressions": ad_slots.astype(int),
            "filled_impressions": filled.astype(int),
            "cpm_usd": cpm.astype(float),
            "revenue_usd": revenue.astype(float),
        }
    )

    return fct_events, fct_ads


def main() -> None:
    cfg = Config(
        seed=int(os.getenv("SEED", "7")),
        days=int(os.getenv("DAYS", "120")),
        n_creators=int(os.getenv("N_CREATORS", "300")),
        n_listeners=int(os.getenv("N_LISTENERS", "50000")),
    )

    dims = generate_dimensions(cfg)
    fct_events, fct_ads = generate_events(cfg, dims)

    os.makedirs("data/generated", exist_ok=True)
    for name, df in dims.items():
        df.to_csv(f"data/generated/{name}.csv", index=False)
        print(f"Wrote data/generated/{name}.csv  rows={len(df):,}")

    fct_events.to_csv("data/generated/fct_events.csv", index=False)
    print(f"Wrote data/generated/fct_events.csv rows={len(fct_events):,}")

    fct_ads.to_csv("data/generated/fct_ads.csv", index=False)
    print(f"Wrote data/generated/fct_ads.csv rows={len(fct_ads):,}")


if __name__ == "__main__":
    main()