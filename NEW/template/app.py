"""
app.py
Flask backend for showing PostGIS layers and exporting selected features as zipped shapefile.
"""

import os
import json
import tempfile
import zipfile
import shutil
from pathlib import Path
from flask import Flask, render_template, jsonify, request, send_file
import psycopg2
import geopandas as gpd

app = Flask(__name__)

# -------------------------
# Database config (your DB)
# -------------------------
DB_CONFIG = {
    "host": "localhost",
    "dbname": "gis_projects",
    "user": "postgres",
    "password": "KIM7222",
    "port": "5432"
}

# -------------------------
# Layers mapping
# Keys are used by the frontend; table is the actual PostGIS table name.
# -------------------------
LAYERS = {
    "adm0": {
        "table": "ken_admbnda_adm0_iebc_20191031",
        "title": "Kenya (ADM0)",
        "type": "polygon",
        "color": "#000000"
    },
    "adm1": {
        "table": "ken_admbnda_adm1_iebc_20191031",
        "title": "Counties (ADM1)",
        "type": "polygon",
        "color": "#0277bd"
    },
    "adm2": {
        "table": "ken_admbnda_adm2_iebc_20191031",
        "title": "Subcounties (ADM2)",
        "type": "polygon",
        "color": "#2e7d32"
    },
    "electoral_poly": {
        "table": "ken_admbndl_admall_iebc_20191031",
        "title": "Electoral Boundaries (Polygons)",
        "type": "polygon",
        "color": "#6a1b9a"
    },
    "points": {
        "table": "ken_admbndp_admall_iebc_itos_20191031",
        "title": "Polling Points",
        "type": "point",
        "color": "#ff5722"
    }
}

# -------------------------
# Helpers
# -------------------------
def get_connection():
    """Open a new psycopg2 connection from DB_CONFIG."""
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        port=DB_CONFIG["port"]
    )

def fetch_geojson_from_table(table_name, geom_col="geom"):
    """
    Build a FeatureCollection with a stable _rowid (1-based row number) included in properties.
    This query returns a JSON object; we load it to return as Python dict later.
    """
    # Protect: only allow known tables
    allowed_tables = {v["table"] for v in LAYERS.values()}
    if table_name not in allowed_tables:
        raise ValueError("Table not recognized")

    sql = f"""
        SELECT jsonb_build_object(
            'type', 'FeatureCollection',
            'features', jsonb_agg(feature)
        ) FROM (
            SELECT jsonb_build_object(
                'type', 'Feature',
                'geometry', ST_AsGeoJSON({geom_col})::jsonb,
                'properties', to_jsonb(t) - '{geom_col}'
            ) AS feature
            FROM (
                SELECT *, row_number() OVER () AS _rowid FROM {table_name}
            ) t
        ) features;
    """

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return {"type": "FeatureCollection", "features": []}
    return row[0]

def fetch_attributes(table_name, limit=None):
    """
    Return columns and rows from table_name. Adds _rowid matching the geojson _rowid.
    limit: optional int to limit rows returned (useful if table is huge).
    """
    allowed_tables = {v["table"] for v in LAYERS.values()}
    if table_name not in allowed_tables:
        raise ValueError("Table not recognized")
    conn = get_connection()
    cur = conn.cursor()
    lim = f"LIMIT {int(limit)}" if limit else ""
    # Select * + row_number to get stable rowid
    sql = f"SELECT *, row_number() OVER () AS _rowid FROM {table_name} {lim};"
    cur.execute(sql)
    colnames = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    # Convert rows to list of dicts with JSON-safe values
    rows_out = []
    for r in rows:
        obj = {}
        for k, v in zip(colnames, r):
            # If geometry column, skip: we'll not include geometry in table
            if k == 'geom':
                continue
            # Convert bytes/other non-jsonable to str
            try:
                json.dumps(v)
                obj[k] = v
            except Exception:
                obj[k] = str(v)
        rows_out.append(obj)
    # Remove geom from column list if present
    cols = [c for c in colnames if c != 'geom']
    return cols, rows_out

# -------------------------
# Routes
# -------------------------
@app.route("/")
def index():
    # Pass LAYERS metadata to the HTML template
    layers_for_template = {k: {"title": v["title"], "type": v["type"], "color": v["color"]} for k, v in LAYERS.items()}
    return render_template("index.html", layers=layers_for_template)

@app.route("/data/<layer_key>")
def data_layer(layer_key):
    """
    Return GeoJSON for the requested layer key (adm0, adm1, adm2, electoral_poly, points).
    """
    if layer_key not in LAYERS:
        return jsonify({"error": "Unknown layer key"}), 404
    table = LAYERS[layer_key]["table"]
    try:
        geojson_obj = fetch_geojson_from_table(table, geom_col="geom")
        return jsonify(geojson_obj)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/attributes/<layer_key>")
def attributes(layer_key):
    """Return attribute columns and rows for DataTables. Adds _rowid for mapping to features."""
    if layer_key not in LAYERS:
        return jsonify({"error": "Unknown layer key"}), 404
    table = LAYERS[layer_key]["table"]
    try:
        cols, rows = fetch_attributes(table, limit=None)
        # Ensure each row has _rowid available (it was included)
        # (fetch_attributes already added _rowid in rows if present)
        return jsonify({"columns": cols, "rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/download", methods=["POST"])
def download_selection():
    """
    Expected JSON payload:
    {
      "layer": "adm1",
      "selected": [1,5,10]   # _rowid values (1-based indices)
    }
    Returns: zipped shapefile of selected features
    """
    payload = request.get_json()
    if not payload:
        return jsonify({"error": "No JSON payload provided"}), 400
    layer_key = payload.get("layer")
    selected = payload.get("selected", [])
    if layer_key not in LAYERS:
        return jsonify({"error": "Invalid layer key"}), 400
    if not isinstance(selected, list) or not selected:
        return jsonify({"error": "No selected features provided"}), 400

    table = LAYERS[layer_key]["table"]

    # Read entire table to a GeoDataFrame, create _rowid and subset
    conn = get_connection()
    try:
        gdf = gpd.read_postgis(f"SELECT * FROM {table}", conn, geom_col="geom")
    finally:
        conn.close()

    if gdf.empty:
        return jsonify({"error": "Layer has no features"}), 400

    gdf = gdf.reset_index(drop=True)
    # _rowid is 1-based
    gdf["_rowid"] = gdf.index + 1

    # Validate selected indices
    sel_valid = [int(x) for x in selected if isinstance(x, int) or (isinstance(x, str) and x.isdigit())]
    sel_valid = [x for x in sel_valid if 1 <= x <= len(gdf)]
    if not sel_valid:
        return jsonify({"error": "No valid selection indices"}), 400

    subset = gdf[gdf["_rowid"].isin(sel_valid)].copy()

    # Create temporary folder to write shapefile
    tmpdir = Path(tempfile.mkdtemp(prefix="export_"))
    shp_basename = f"{table}_selection"
    out_path = tmpdir / shp_basename

    try:
        # geopandas writes a set of files: .shp .shx .dbf .prj
        subset.to_file(str(out_path) + ".shp", driver="ESRI Shapefile")
        # Zip files that begin with shp_basename
        zip_path = tmpdir / (shp_basename + ".zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in tmpdir.iterdir():
                if f.is_file() and f.name.startswith(shp_basename):
                    zf.write(f, arcname=f.name)
        # Send zip file
        return send_file(str(zip_path), as_attachment=True, download_name=shp_basename + ".zip")
    except Exception as e:
        return jsonify({"error": f"Export failed: {e}"}), 500
    finally:
        # cleanup tempdir (do not remove immediately because send_file may still be streaming).
        # We'll schedule deletion: best-effort immediate cleanup.
        try:
            shutil.rmtree(tmpdir)
        except Exception:
            pass

# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    # recommend running with a production WSGI for public deployment
    app.run(debug=True, host="127.0.0.1", port=5000)
