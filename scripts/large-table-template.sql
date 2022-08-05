-- Template for generating a large table
DROP TABLE IF EXISTS "TrackAll";

CREATE TABLE "TrackAll" AS (
    {% for _ in range(1000) %}
        SELECT * FROM "Track"
        {% if not loop.last %}
        UNION ALL
    {% endif %}
    {% endfor %}

);


SELECT COUNT(*) "TrackAll";