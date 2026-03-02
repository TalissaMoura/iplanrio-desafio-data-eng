from flask import Blueprint, request, jsonify
from app.db import get_connection

terceirizados_bp = Blueprint("terceirizados", __name__)

DEFAULT_LIMIT = 50
MAX_LIMIT = 200


@terceirizados_bp.route("/terceirizados", methods=["GET"])
def list_terceirizados():
    try:
        b_start = int(request.args.get("b_start", 0))
        limit = int(request.args.get("limit", DEFAULT_LIMIT))
    except ValueError:
        return jsonify({"error": "Parâmetros inválidos"}), 400

    if b_start < 0:
        return jsonify({"error": "b_start deve ser >= 0"}), 400

    if limit > MAX_LIMIT:
        limit = MAX_LIMIT

    conn = get_connection()

    total = conn.execute("SELECT COUNT(*) FROM app_terceirizados").fetchone()[0]

    rows = conn.execute(
        """
        SELECT *
        FROM app_terceirizados
        ORDER BY id
        LIMIT ?
        OFFSET ?
        """,
        [limit, b_start],
    ).fetchdf()

    conn.close()

    next_start = b_start + limit if (b_start + limit) < total else None

    return jsonify(
        {
            "b_start": b_start,
            "limit": limit,
            "total": total,
            "next": next_start,
            "data": rows.to_dict(orient="records"),
        }
    )
