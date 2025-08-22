#!/usr/bin/env python3

import argparse
import sys
from typing import Tuple, List

try:
    import pandas as pd
except ImportError as import_error:
    sys.stderr.write(
        "Missing dependency: pandas. Install it with `python -m pip install pandas` and rerun.\n"
    )
    raise


REQUIRED_COLUMNS: List[str] = ["h", "n", "rho", "gamma", "v", "df"]
DEFAULT_NODE_A: str = "LGD_LANGFORD"
DEFAULT_NODE_B: str = "TRSES_UNIT6"


def validate_columns(frame: "pd.DataFrame") -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in frame.columns]
    if missing:
        raise ValueError(
            f"Input CSV is missing required columns: {', '.join(missing)}. "
            f"Required columns are: {', '.join(REQUIRED_COLUMNS)}"
        )


def compute_hourly_values(
    frame: "pd.DataFrame",
    node_i: str,
    node_j: str,
) -> "pd.DataFrame":
    """
    Compute hourly path values v(i,j,h) = sum_{rho,gamma} v(rho,gamma,h) * (df(i,rho,gamma) - df(j,rho,gamma)).

    Returns a DataFrame with columns: [h, path, v_hourly]. The two paths returned are:
    - (node_i, node_j)
    - (node_j, node_i)
    """
    # Filter to relevant nodes only to avoid unnecessary pivot width and memory
    filtered = frame[frame["n"].isin([node_i, node_j])].copy()

    # Ensure numeric types where appropriate
    for col in ["h", "v", "df"]:
        if col in filtered.columns:
            filtered[col] = pd.to_numeric(filtered[col], errors="coerce")

    # Drop rows with missing critical values
    filtered = filtered.dropna(subset=["h", "rho", "gamma", "n", "v", "df"])  # type: ignore[arg-type]

    if filtered.empty:
        raise ValueError(
            "No usable rows found after filtering for required columns and specified nodes."
        )

    # For each (h, rho, gamma), we need df for node_i and node_j, and the price v(rho,gamma,h)
    # We pivot df by node
    df_pivot = (
        filtered.pivot_table(
            index=["h", "rho", "gamma"],
            columns="n",
            values="df",
            aggfunc="mean",  # if duplicates exist, mean is acceptable under linearity
        )
        .reset_index()
    )

    # Merge back a representative price per (h,rho,gamma). If duplicated, take mean and warn later if conflicts
    price_series = (
        filtered.groupby(["h", "rho", "gamma"], as_index=False)["v"].mean()
    )

    merged = df_pivot.merge(price_series, on=["h", "rho", "gamma"], how="inner")

    # Keep only groups where at least one of the nodes' df exists. Missing df treated as 0
    if node_i not in merged.columns:
        merged[node_i] = 0.0
    if node_j not in merged.columns:
        merged[node_j] = 0.0

    # Flow contribution f(i,j,rho,gamma) for each (h,rho,gamma)
    merged["f_i_j"] = merged[node_i].fillna(0.0) - merged[node_j].fillna(0.0)

    # Contribution to value for the forward path (i -> j)
    merged["contrib_forward"] = merged["v"] * merged["f_i_j"]

    # Hourly sum across all (rho,gamma)
    hourly_forward = (
        merged.groupby("h", as_index=False)["contrib_forward"].sum().rename(columns={"contrib_forward": "v_hourly"})
    )
    hourly_forward["path"] = [f"({node_i}, {node_j})"] * len(hourly_forward)

    # Reverse path (j -> i) has negated f(i,j) and thus negated contributions
    hourly_reverse = hourly_forward.copy()
    hourly_reverse["v_hourly"] = -hourly_reverse["v_hourly"]
    hourly_reverse["path"] = [f"({node_j}, {node_i})"] * len(hourly_reverse)

    # Combine and sort by hour then path for determinism
    hourly_both = pd.concat([hourly_forward, hourly_reverse], ignore_index=True)
    hourly_both = hourly_both.sort_values(by=["h", "path"]).reset_index(drop=True)

    # Reorder columns
    hourly_both = hourly_both[["h", "path", "v_hourly"]]

    return hourly_both


def compute_totals(summary_frame: "pd.DataFrame") -> "pd.DataFrame":
    """
    Given per-hour path values with columns [h, path, v_hourly], compute:
    - vobl(path) = sum_h v_hourly
    - vopt(path) = sum_h max(v_hourly, 0)
    Returns DataFrame with columns: [path, vobl, vopt]
    """
    grouped = summary_frame.groupby("path", as_index=False)
    totals = grouped.agg(
        vobl=("v_hourly", "sum"),
        vopt=("v_hourly", lambda s: s.clip(lower=0).sum()),
    )
    return totals


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute hourly path values for (LGD_LANGFORD, TRSES_UNIT6) and its reverse "
            "from a dataset CSV containing columns h, n, rho, gamma, v, df."
        )
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input CSV with columns: h, n, rho, gamma, v, df",
    )
    parser.add_argument(
        "--output-hourly",
        required=True,
        help="Path to write per-hour path values CSV (columns: h, path, v_hourly)",
    )
    parser.add_argument(
        "--node-i",
        default=DEFAULT_NODE_A,
        help=f"Source node for forward path (default: {DEFAULT_NODE_A})",
    )
    parser.add_argument(
        "--node-j",
        default=DEFAULT_NODE_B,
        help=f"Sink node for forward path (default: {DEFAULT_NODE_B})",
    )
    parser.add_argument(
        "--print-totals",
        action="store_true",
        help="If set, prints vobl and vopt totals per path to stdout",
    )
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    # Read CSV
    try:
        frame = pd.read_csv(args.input)
    except FileNotFoundError:
        sys.stderr.write(f"Input file not found: {args.input}\n")
        return 2
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"Failed to read CSV: {exc}\n")
        return 2

    # Validate schema
    try:
        validate_columns(frame)
    except ValueError as ve:
        sys.stderr.write(str(ve) + "\n")
        return 2

    # Compute hourly values
    try:
        hourly = compute_hourly_values(frame, args.node_i, args.node_j)
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"Computation failed: {exc}\n")
        return 3

    # Write output
    try:
        hourly.to_csv(args.output_hourly, index=False)
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"Failed to write hourly output CSV: {exc}\n")
        return 4

    # Optionally print totals
    if args.print_totals:
        totals = compute_totals(hourly)
        # Pretty print to stdout
        for _, row in totals.iterrows():
            path = row["path"]
            vobl = row["vobl"]
            vopt = row["vopt"]
            print(f"{path}: vobl={vobl:.6f}, vopt={vopt:.6f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))