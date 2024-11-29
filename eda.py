# %%
import polars as pl
import altair as alt

from google.cloud import bigquery

# %%
# Color palette:
CA_01 = "#D12030"
CA_02 = "#F17A3B"
CA_03 = "#9B4A97"
CA_04 = "#2E86AB"
CA_05 = "#2A4494"
CA_06 = "#5E2F50"
CA_07 = "#E76D83"
CA_08 = "#7EA8BE"
CA_09 = "#B8D4E3"
CA_10 = "#F5E6E8"
CA_11 = "#FFFFFF"

COLORS = [CA_01, CA_02, CA_03, CA_04, CA_05, CA_06, CA_07, CA_08, CA_09, CA_10]

# %%
client = bigquery.Client(project="aol-bva-examen-443118")
traffic = bigquery.DatasetReference(client.project, "traffic_data")
job_config = bigquery.QueryJobConfig()

# %%
# fecha_evento                   Type: DATE       Mode: REQUIRED
# hora_evento                    Type: TIME       Mode: NULLABLE
# tipo_evento                    Type: INTEGER    Mode: REQUIRED
# fecha_captura                  Type: DATE       Mode: NULLABLE
# folio                          Type: STRING     Mode: NULLABLE
# latitud                        Type: FLOAT      Mode: NULLABLE
# longitud                       Type: FLOAT      Mode: NULLABLE
# punto_1                        Type: STRING     Mode: REQUIRED
# punto_2                        Type: STRING     Mode: NULLABLE
# colonia                        Type: STRING     Mode: REQUIRED
# alcaldia                       Type: INTEGER    Mode: REQUIRED
# zona_vial                      Type: INTEGER    Mode: REQUIRED
# sector                         Type: STRING     Mode: REQUIRED
# unidad_cargo                   Type: STRING     Mode: NULLABLE
# tipo_interseccion              Type: INTEGER    Mode: REQUIRED
# interseccion_semaforizada      Type: BOOLEAN    Mode: REQUIRED
# clasificacion_vialidad         Type: INTEGER    Mode: NULLABLE
# sentido_circulacion            Type: INTEGER    Mode: NULLABLE
# dia                            Type: INTEGER    Mode: REQUIRED
# prioridad                      Type: INTEGER    Mode: REQUIRED
# origen                         Type: INTEGER    Mode: NULLABLE
# unidad_medica_apoyo            Type: STRING     Mode: NULLABLE
# matricula_unidad_medica        Type: STRING     Mode: NULLABLE
# trasladado_lesionados          Type: BOOLEAN    Mode: REQUIRED
# personas_fallecidas            Type: INTEGER    Mode: REQUIRED
# personas_lesionadas            Type: INTEGER    Mode: REQUIRED

events_ref = client.dataset("traffic_data").table("events")
events_table = client.get_table(events_ref)
for field in events_table.schema:
    print(f"{field.name:<30} Type: {field.field_type:<10} Mode: {field.mode}")


# %%
def query(query: str) -> pl.DataFrame:
    global client, traffic

    query_job = client.query(query.replace("#", f"{traffic}"), job_config=job_config)
    return pl.DataFrame(pl.from_arrow(query_job.result().to_arrow()))

# %%
# Number of fallecimientos and lesionados per year
res = query("""
SELECT
    EXTRACT(YEAR FROM fecha_evento) as anio,
    SUM(personas_fallecidas) as total_fallecidos,
    SUM(personas_lesionadas) as total_lesionados
FROM `#.events`
GROUP BY anio
ORDER BY anio
""")

res_alt = (
    res.with_columns(pl.col("anio").alias("Año"))
    .with_columns(pl.col("total_fallecidos").alias("Fallecidos"))
    .with_columns(pl.col("total_lesionados").alias("Lesionados"))
)

chart = (
    (alt.layer(
        alt.Chart(res_alt)
        .mark_line(point={"color": "black"}, color=CA_01)
        .encode(
            x=alt.X("Año:O", title="Año"),
            y=alt.Y("Fallecidos:Q", title="Fallecidos", axis=alt.Axis(format="s")),
            tooltip=["Año", alt.Tooltip("Fallecidos:Q", format=",", title="Fallecidos")]
        ),
        alt.Chart(res_alt)
        .mark_line(point={"color": "black"}, color=CA_04)
        .encode(
            x=alt.X("Año:O"),
            y=alt.Y("Lesionados:Q", title="Lesionados", axis=alt.Axis(format="s")),
            tooltip=["Año", alt.Tooltip("Lesionados:Q", format=",", title="Lesionados")]
        ),

        alt.Chart(res_alt)
        .mark_text(align="center", baseline="bottom", dy=-5, fontSize=10)
        .encode(
            x=alt.X("Año:O"),
            y=alt.Y("Fallecidos:Q", axis=None),
            text=alt.Text("Fallecidos:Q", format=",")
        ),

        alt.Chart(res_alt)
        .mark_text(align="center", baseline="bottom", dy=-5, fontSize=10)
        .encode(
            x=alt.X("Año:O"),
            y=alt.Y("Lesionados:Q", axis=None),
            text=alt.Text("Lesionados:Q", format=",")
        )
    )
    .resolve_scale(y='independent')
    .properties(
        width=600,
        height=400,
        title="Fallecidos y Lesionados por Año"
    ))
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# peoplpe injured per accidents / deaths ratio by year
res = query("""
WITH accidents_by_year AS (
    SELECT
        EXTRACT(YEAR FROM fecha_evento) as anio,
        COUNT(*) as total_accidents,
        SUM(personas_fallecidas) as total_deaths
    FROM `#.events`
    GROUP BY anio
)
SELECT
    anio,
    total_accidents,
    total_deaths,
    CAST(total_accidents AS FLOAT64) / NULLIF(total_deaths, 0) as ratio
FROM accidents_by_year
ORDER BY anio
""")

res_alt = res.with_columns(pl.col("anio").alias("Año")).with_columns(
    pl.col("ratio").alias("Ratio")
)

chart = (
    alt.layer(
        alt.Chart(res_alt)
        .mark_line(point=True)
        .encode(
            x=alt.X("Año:O", title="Año"),
            y=alt.Y("Ratio:Q", title="Personas Accidentadas por Muerte", axis=alt.Axis(format=",.0f")),
            tooltip=[
                "Año",
                alt.Tooltip("Ratio:Q", format=",.1f"),
                alt.Tooltip("total_accidents:Q", title="Total Personas en Accidentes", format=","),
                alt.Tooltip("total_deaths:Q", title="Total Personas Fallecidas", format=",")
            ]
        ),
        alt.Chart(res_alt)
        .mark_text(align="center", baseline="bottom", dy=-10, fontSize=10)
        .encode(
            x=alt.X("Año:O"),
            y=alt.Y("Ratio:Q"),
            text=alt.Text("total_deaths:Q", format=",")
        ),
        alt.Chart(res_alt)
        .mark_text(align="center", baseline="top", dy=10, fontSize=10)
        .encode(
            x=alt.X("Año:O"),
            y=alt.Y("Ratio:Q"),
            text=alt.Text("total_accidents:Q", format=",")
        )
    )
    .properties(
        width=600,
        height=400,
        title="Razón de Personas Accidentadas por cada Fallecida"
    )
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Ratio of injured people per death by year and tipo
res = query("""
WITH accidents_by_year_tipo AS (
    SELECT
        EXTRACT(YEAR FROM fecha_evento) as anio,
        t.tipo_evento as tipo,
        COUNT(*) as total_accidents,
        SUM(personas_fallecidas) as total_deaths,
        SUM(personas_lesionadas) as total_lesionados
    FROM `#.events` e
    JOIN `#.tipo_evento` t ON e.tipo_evento = t.id
    GROUP BY anio, tipo
)
SELECT
    anio,
    tipo,
    total_accidents,
    total_deaths,
    total_lesionados,
    CAST(total_lesionados AS FLOAT64) / NULLIF(total_deaths, 0) as ratio
FROM accidents_by_year_tipo
ORDER BY anio, tipo
""")

res_alt = (
    res.with_columns(pl.col("anio").alias("Año"))
    .with_columns(pl.col("tipo").alias("Tipo"))
    .with_columns(pl.col("ratio").alias("Ratio"))
)

chart = (
    alt.layer(
        alt.Chart(res_alt)
        .mark_line(point=True)
        .encode(
            x=alt.X("Año:O", title="Año"),
            y=alt.Y("Ratio:Q", title="Personas Lesionadas por Muerte", axis=alt.Axis(format=",.0f")),
            color=alt.Color("Tipo:N", scale=alt.Scale(range=COLORS), legend=alt.Legend(title="Tipo de Evento")),
            tooltip=[
                "Año",
                "Tipo",
                alt.Tooltip("Ratio:Q", format=",.1f"),
                alt.Tooltip("total_accidents:Q", title="Total Accidentes", format=","),
                alt.Tooltip("total_deaths:Q", title="Total Fallecidos", format=","),
                alt.Tooltip("total_lesionados:Q", title="Total Lesionados", format=",")
            ]
        ),
        alt.Chart(res_alt)
        .mark_text(align="center", baseline="bottom", dy=-10, fontSize=10)
        .encode(
            x=alt.X("Año:O"),
            y=alt.Y("Ratio:Q"),
            text=alt.Text("total_deaths:Q", format=",")
        ),
        alt.Chart(res_alt)
        .mark_text(align="center", baseline="top", dy=10, fontSize=10)
        .encode(
            x=alt.X("Año:O"),
            y=alt.Y("Ratio:Q"),
            text=alt.Text("total_lesionados:Q", format=",")
        )
    )
    .properties(
        width=600,
        height=400,
        title="Razón de Personas Lesionadas por cada Fallecida por Tipo de Evento"
    )
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Gráfica de líneas que muestra el número de fallecimientos por tipo de evento a lo largo de años.
res = query("""
SELECT
    EXTRACT(YEAR FROM fecha_evento) as anio,
    t.tipo_evento as tipo,
    SUM(personas_fallecidas) as personas_fallecidas,
 FROM `#.events` e
 JOIN `#.tipo_evento` t ON e.tipo_evento = t.id
GROUP BY anio, tipo
""")


res_alt = (
    res.with_columns(pl.col("anio").alias("Año"))
    .with_columns(pl.col("tipo").alias("Tipo"))
    .with_columns(pl.col("personas_fallecidas").alias("Fallecimientos"))
)

chart = (
    alt.Chart(res_alt)
    .mark_line(point=True, strokeWidth=3)
    .encode(
        x=alt.X("Año:O", title="Año"),
        y=alt.Y(
            "Fallecimientos:Q",
            title="Número de Fallecimientos",
            axis=alt.Axis(format="s"),
        ),
        color=alt.Color(
            "Tipo:N",
            scale=alt.Scale(range=COLORS),
            legend=alt.Legend(title="Tipo de Evento"),
        ),
        tooltip=["Año", "Tipo", alt.Tooltip("Fallecimientos:Q", format=",")],
    )
    .properties(width=600, height=400, title="Fallecimientos por Año y Tipo de Evento")
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Gráfica de líneas que muestra el número de heridos por tipo de evento a lo largo de años.
res = query("""
SELECT
    EXTRACT(YEAR FROM fecha_evento) as anio,
    t.tipo_evento as tipo,
    SUM(personas_lesionadas) as personas_lesionadas,
 FROM `#.events` e
 JOIN `#.tipo_evento` t ON e.tipo_evento = t.id
GROUP BY anio, tipo
""")


res_alt = (
    res.with_columns(pl.col("anio").alias("Año"))
    .with_columns(pl.col("tipo").alias("Tipo"))
    .with_columns(pl.col("personas_lesionadas").alias("Lesionados"))
)

chart = (
    alt.Chart(res_alt)
    .mark_line(point=True, strokeWidth=3)
    .encode(
        x=alt.X("Año:O", title="Año"),
        y=alt.Y(
            "Lesionados:Q", title="Número de Lesionados", axis=alt.Axis(format="s")
        ),
        color=alt.Color(
            "Tipo:N",
            scale=alt.Scale(range=COLORS),
            legend=alt.Legend(title="Tipo de Evento"),
        ),
        tooltip=["Año", "Tipo", alt.Tooltip("Lesionados:Q", format=",")],
    )
    .properties(width=600, height=400, title="Lesionados por Año y Tipo de Evento")
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Gráfica de frecuencia de eventos por origen
res = query("""
SELECT
    o.origen,
    COUNT(*) as total_eventos
FROM `#.events` e
JOIN `#.origen` o ON e.origen = o.id
GROUP BY o.origen
""")

# Group small values into "Otros"
THRESHOLD = 700  # Adjust this threshold as needed
total_sum = res["total_eventos"].sum()
res_alt = (
    res.with_columns(
        pl.when(pl.col("total_eventos") < THRESHOLD)
        .then(pl.lit("Otros"))
        .otherwise(pl.col("origen"))
        .alias("Origen")
    )
    .group_by("Origen")
    .agg(pl.col("total_eventos").sum().alias("Total"))
)

chart = (
    alt.Chart(res_alt)
    .mark_bar()
    .encode(
        x=alt.X("Origen:N", title="Origen", sort="-y"),
        y=alt.Y("Total:Q", title="Número de Eventos", axis=alt.Axis(format="s")),
        color=alt.Color("Origen:N", scale=alt.Scale(range=COLORS), legend=None),
        tooltip=["Origen", alt.Tooltip("Total:Q", format=",")],
    )
    .properties(width=600, height=400, title="Distribución de Eventos por Origen")
    .configure_axis(labelFontSize=12, titleFontSize=14, labelAngle=45)
    .configure_title(fontSize=16)
)

chart

# Distribution of events by origen over time
res = query("""
SELECT
    EXTRACT(YEAR FROM fecha_evento) as anio,
    o.origen,
    COUNT(*) as total_eventos
FROM `#.events` e
JOIN `#.origen` o ON e.origen = o.id
GROUP BY anio, o.origen
""")

# Group small values into "Otros"
total_by_origen = res.group_by("origen").agg(pl.col("total_eventos").sum())
THRESHOLD = 1000  # Adjust threshold as needed

res_alt = (
    res.with_columns(
        pl.when(
            pl.col("origen").is_in(
                total_by_origen.filter(pl.col("total_eventos") < THRESHOLD)["origen"]
            )
        )
        .then(pl.lit("Otros"))
        .otherwise(pl.col("origen"))
        .alias("Origen"),
        pl.col("anio").alias("Año"),
    )
    .group_by(["Año", "Origen"])
    .agg(pl.col("total_eventos").sum().alias("Total"))
)

chart = (
    alt.Chart(res_alt)
    .mark_bar()
    .encode(
        x=alt.X("Año:O", title="Año"),
        y=alt.Y(
            "Total:Q",
            title="Número de Eventos",
            axis=alt.Axis(format="s"),
            stack="normalize",
        ),
        color=alt.Color(
            "Origen:N", scale=alt.Scale(range=COLORS), legend=alt.Legend(title="Origen")
        ),
        tooltip=["Año", "Origen", alt.Tooltip("Total:Q", format=",")],
    )
    .properties(width=600, height=400, title="Distribución de Eventos por Origen y Año")
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Distribution of events by origen over time
res = query("""
SELECT
    EXTRACT(YEAR FROM fecha_evento) as anio,
    o.origen,
    COUNT(*) as total_eventos
FROM `#.events` e
JOIN `#.origen` o ON e.origen = o.id
GROUP BY anio, o.origen
""")

# Group small values into "Otros"
total_by_origen = res.group_by("origen").agg(pl.col("total_eventos").sum())
THRESHOLD = 1000  # Adjust threshold as needed

res_alt = (
    res.with_columns(
        pl.when(
            pl.col("origen").is_in(
                total_by_origen.filter(pl.col("total_eventos") < THRESHOLD)["origen"]
            )
        )
        .then(pl.lit("Otros"))
        .otherwise(
            pl.when(pl.col("origen") == "911 CDMX")
            .then(pl.lit("LLAMADA DEL 911"))
            .otherwise(pl.col("origen"))
        )
        .alias("Origen"),
        pl.col("anio").alias("Año"),
    )
    .group_by(["Año", "Origen"])
    .agg(pl.col("total_eventos").sum().alias("Total"))
)

chart = (
    alt.Chart(res_alt)
    .mark_bar()
    .encode(
        x=alt.X("Año:O", title="Año"),
        y=alt.Y(
            "Total:Q",
            title="Número de Eventos",
            axis=alt.Axis(format="s"),
            stack="normalize",
        ),
        color=alt.Color(
            "Origen:N", scale=alt.Scale(range=COLORS), legend=alt.Legend(title="Origen")
        ),
        tooltip=["Año", "Origen", alt.Tooltip("Total:Q", format=",")],
    )
    .properties(width=600, height=400, title="Distribución de Eventos por Origen y Año")
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Number of accidents per hour
res = query("""
SELECT
    EXTRACT(HOUR FROM hora_evento) as hora,
    COUNT(*) as total_accidents
FROM `#.events` e
GROUP BY hora
ORDER BY hora
""")

res_alt = res.with_columns(pl.col("hora").alias("Hora")).with_columns(
    pl.col("total_accidents").alias("Total")
)

chart = (
    alt.Chart(res_alt)
    .mark_line(point=True)
    .encode(
        x=alt.X("Hora:Q", title="Hora del día"),
        y=alt.Y("Total:Q", title="Número de Accidentes", axis=alt.Axis(format="s")),
        tooltip=["Hora", alt.Tooltip("Total:Q", format=",")],
    )
    .properties(width=600, height=400, title="Distribución de Accidentes por Hora")
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Deaths per hour
res = query("""
SELECT
    EXTRACT(HOUR FROM hora_evento) as hora,
    SUM(personas_fallecidas) as total_fallecidos
FROM `#.events` e
GROUP BY hora
ORDER BY hora
""")

res_alt = res.with_columns(pl.col("hora").alias("Hora")).with_columns(
    pl.col("total_fallecidos").alias("Total")
)

chart = (
    alt.Chart(res_alt)
    .mark_line(point=True)
    .encode(
        x=alt.X("Hora:Q", title="Hora del día"),
        y=alt.Y("Total:Q", title="Número de Fallecidos", axis=alt.Axis(format="s")),
        tooltip=["Hora", alt.Tooltip("Total:Q", format=",")],
    )
    .properties(width=600, height=400, title="Distribución de Fallecidos por Hora")
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Injured per hour
res = query("""
SELECT
    EXTRACT(HOUR FROM hora_evento) as hora,
    SUM(personas_lesionadas) as total_lesionados
FROM `#.events` e
GROUP BY hora
ORDER BY hora
""")

res_alt = res.with_columns(pl.col("hora").alias("Hora")).with_columns(
    pl.col("total_lesionados").alias("Total")
)

chart = (
    alt.Chart(res_alt)
    .mark_line(point=True)
    .encode(
        x=alt.X("Hora:Q", title="Hora del día"),
        y=alt.Y("Total:Q", title="Número de Lesionados", axis=alt.Axis(format="s")),
        tooltip=["Hora", alt.Tooltip("Total:Q", format=",")],
    )
    .properties(width=600, height=400, title="Distribución de Lesionados por Hora")
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Accidents per hour by year
res = query("""
SELECT
    EXTRACT(YEAR FROM fecha_evento) as anio,
    EXTRACT(HOUR FROM hora_evento) as hora,
    COUNT(*) as total_accidentes
FROM `#.events` e
GROUP BY anio, hora
ORDER BY anio, hora
""")

res_alt = (
    res.with_columns(pl.col("anio").alias("Año"))
    .with_columns(pl.col("hora").alias("Hora"))
    .with_columns(pl.col("total_accidentes").alias("Total"))
)

chart = (
    alt.Chart(res_alt)
    .mark_line(point=True)
    .encode(
        x=alt.X("Hora:Q", title="Hora del día"),
        y=alt.Y("Total:Q", title="Número de Accidentes", axis=alt.Axis(format="s")),
        color=alt.Color(
            "Año:N", scale=alt.Scale(range=COLORS), legend=alt.Legend(title="Año")
        ),
        tooltip=["Año", "Hora", alt.Tooltip("Total:Q", format=",")],
    )
    .properties(
        width=600, height=400, title="Distribución de Accidentes por Hora y Año"
    )
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Deaths per hour by year
res = query("""
SELECT
    EXTRACT(YEAR FROM fecha_evento) as anio,
    EXTRACT(HOUR FROM hora_evento) as hora,
    SUM(personas_fallecidas) as total_fallecidos
FROM `#.events` e
GROUP BY anio, hora
ORDER BY anio, hora
""")

res_alt = (
    res.with_columns(pl.col("anio").alias("Año"))
    .with_columns(pl.col("hora").alias("Hora"))
    .with_columns(pl.col("total_fallecidos").alias("Total"))
)

chart = (
    alt.Chart(res_alt)
    .mark_line(point=True)
    .encode(
        x=alt.X("Hora:Q", title="Hora del día"),
        y=alt.Y("Total:Q", title="Número de Fallecidos", axis=alt.Axis(format="s")),
        color=alt.Color(
            "Año:N", scale=alt.Scale(range=COLORS), legend=alt.Legend(title="Año")
        ),
        tooltip=["Año", "Hora", alt.Tooltip("Total:Q", format=",")],
    )
    .properties(
        width=600, height=400, title="Distribución de Fallecidos por Hora y Año"
    )
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Injured per hour by year
res = query("""
SELECT
    EXTRACT(YEAR FROM fecha_evento) as anio,
    EXTRACT(HOUR FROM hora_evento) as hora,
    SUM(personas_lesionadas) as total_lesionados
FROM `#.events` e
GROUP BY anio, hora
ORDER BY anio, hora
""")

res_alt = (
    res.with_columns(pl.col("anio").alias("Año"))
    .with_columns(pl.col("hora").alias("Hora"))
    .with_columns(pl.col("total_lesionados").alias("Total"))
)

chart = (
    alt.Chart(res_alt)
    .mark_line(point=True)
    .encode(
        x=alt.X("Hora:Q", title="Hora del día"),
        y=alt.Y("Total:Q", title="Número de Lesionados", axis=alt.Axis(format="s")),
        color=alt.Color(
            "Año:N", scale=alt.Scale(range=COLORS), legend=alt.Legend(title="Año")
        ),
        tooltip=["Año", "Hora", alt.Tooltip("Total:Q", format=",")],
    )
    .properties(
        width=600, height=400, title="Distribución de Lesionados por Hora y Año"
    )
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Number of incidents by tipo at each hour
res = query("""
SELECT
    EXTRACT(HOUR FROM hora_evento) as hora,
    t.tipo_evento as tipo,
    COUNT(*) as total_incidentes
FROM `#.events` e
JOIN `#.tipo_evento` t ON e.tipo_evento = t.id
GROUP BY hora, tipo
ORDER BY hora, tipo
""")

res_alt = (
    res.with_columns(pl.col("hora").alias("Hora"))
    .with_columns(pl.col("tipo").alias("Tipo"))
    .with_columns(pl.col("total_incidentes").alias("Total"))
)

chart = (
    alt.Chart(res_alt)
    .mark_line(point=True)
    .encode(
        x=alt.X("Hora:Q", title="Hora del día"),
        y=alt.Y("Total:Q", title="Número de Incidentes", axis=alt.Axis(format="s")),
        color=alt.Color(
            "Tipo:N",
            scale=alt.Scale(range=COLORS),
            legend=alt.Legend(title="Tipo de Evento"),
        ),
        tooltip=["Hora", "Tipo", alt.Tooltip("Total:Q", format=",")],
    )
    .properties(
        width=600, height=400, title="Distribución de Incidentes por Hora y Tipo"
    )
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# %%
# Number of incidents by tipo at each hour by year
res = query("""
SELECT
    EXTRACT(YEAR FROM fecha_evento) as anio,
    EXTRACT(HOUR FROM hora_evento) as hora,
    t.tipo_evento as tipo,
    COUNT(*) as total_incidentes
FROM `#.events` e
JOIN `#.tipo_evento` t ON e.tipo_evento = t.id
GROUP BY anio, hora, tipo
ORDER BY anio, hora, tipo
""")

res_alt = (
    res.with_columns(pl.col("anio").alias("Año"))
    .with_columns(pl.col("hora").alias("Hora"))
    .with_columns(pl.col("tipo").alias("Tipo"))
    .with_columns(pl.col("total_incidentes").alias("Total"))
)

facet_chart = (
    alt.Chart(res_alt)
    .mark_line()
    .encode(
        x=alt.X("Hora:Q", title="Hora del día"),
        y=alt.Y(
            "Total:Q", axis=alt.Axis(format="s"), scale=alt.Scale(domain=[0, 1000])
        ),
        color=alt.Color(
            "Tipo:N",
            scale=alt.Scale(range=COLORS),
            legend=alt.Legend(title="Tipo de Evento"),
        ),
        tooltip=["Año", "Hora", "Tipo", alt.Tooltip("Total:Q", format=",")],
    )
    .properties(height=100, width=500)
    .facet(
        row=alt.Row(
            "Año:N",
            title="Año",
            sort="descending",
            header=alt.Header(labelOrient="left"),
        ),
        title=alt.TitleParams(
            "Distribución de Incidentes por Hora y Tipo por Año", anchor="middle"
        ),
    )
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
    .resolve_scale(y="shared")
    .configure_facet(spacing=10)
    .properties(title={"text": "Número de Incidentes", "anchor": "middle", "dx": -50})
)

facet_chart

# %%
# Number of accidents per year by whether there was a transport to hospital
res = query("""
SELECT
    EXTRACT(YEAR FROM fecha_evento) as anio,
    trasladado_lesionados,
    COUNT(*) as total_accidentes
FROM `#.events` e
GROUP BY anio, trasladado_lesionados
ORDER BY anio, trasladado_lesionados
""")

res_alt = (
    res.with_columns(pl.col("anio").alias("Año"))
    .with_columns(
        pl.when(pl.col("trasladado_lesionados"))
        .then(pl.lit("Si"))
        .otherwise(pl.lit("No"))
        .alias("Traslado")
    )
    .with_columns(pl.col("total_accidentes").alias("Total"))
)

chart = (
    alt.Chart(res_alt)
    .mark_bar()
    .encode(
        x=alt.X("Año:O", title="Año"),
        y=alt.Y("Total:Q", title="Número de Accidentes", axis=alt.Axis(format="s")),
        color=alt.Color(
            "Traslado:N",
            scale=alt.Scale(range=[CA_01, CA_04]),
            legend=alt.Legend(title="Traslado a Hospital"),
        ),
        tooltip=["Año", "Traslado", alt.Tooltip("Total:Q", format=",")],
    )
    .properties(
        width=600, height=400, title="Distribución de Accidentes por Año y Traslado"
    )
    .configure_axis(labelFontSize=12, titleFontSize=14)
    .configure_title(fontSize=16)
)

chart

# Number of accidents per alcaldia that had a trasladado_lesionados to hospital
res = query("""
SELECT
    a.alcaldia,
    COUNT(*) as total_accidentes
FROM `#.events` e
JOIN `#.alcaldia` a ON e.alcaldia = a.id
WHERE trasladado_lesionados = TRUE
GROUP BY a.alcaldia
ORDER BY total_accidentes DESC
""")

res_alt = res.with_columns(
    pl.when(pl.col("alcaldia") == "GUSTAVO A MADERO")
    .then(pl.lit("GUSTAVO A. MADERO"))
    .otherwise(pl.col("alcaldia"))
    .alias("Alcaldía")
).with_columns(pl.col("total_accidentes").alias("Total"))

chart = (
    alt.Chart(res_alt)
    .mark_bar()
    .encode(
        x=alt.X("Alcaldía:N", title="Alcaldía", sort="-y"),
        y=alt.Y("Total:Q", title="Número de Accidentes", axis=alt.Axis(format="s")),
        color=alt.Color("Alcaldía:N", scale=alt.Scale(range=COLORS), legend=None),
        tooltip=["Alcaldía", alt.Tooltip("Total:Q", format=",")],
    )
    .properties(
        width=600, height=400, title="Accidentes con Traslado a Hospital por Alcaldía"
    )
    .configure_axis(labelFontSize=12, titleFontSize=14, labelAngle=45)
    .configure_title(fontSize=16)
)

chart

# %%
# plot for percentage of accidents that required trasladado_lesionados by alcaldia
res = query("""
WITH total_by_alcaldia AS (
    SELECT
        a.alcaldia,
        COUNT(*) as total
    FROM `#.events` e
    JOIN `#.alcaldia` a ON e.alcaldia = a.id
    GROUP BY a.alcaldia
),
traslados_by_alcaldia AS (
    SELECT
        a.alcaldia,
        COUNT(*) as traslados
    FROM `#.events` e
    JOIN `#.alcaldia` a ON e.alcaldia = a.id
    WHERE trasladado_lesionados = TRUE
    GROUP BY a.alcaldia
)
SELECT
    t.alcaldia,
    t.traslados,
    ta.total,
    t.traslados / ta.total as porcentaje
FROM traslados_by_alcaldia t
JOIN total_by_alcaldia ta ON t.alcaldia = ta.alcaldia
ORDER BY porcentaje DESC
""")

res_alt = (
    res.filter(pl.col("alcaldia") != pl.lit("GUSTAVO A. MADERO"))
    .with_columns(pl.col("alcaldia").alias("Alcaldía"))
    .with_columns(pl.col("porcentaje").alias("Porcentaje"))
)

chart = (
    (
        alt.Chart(res_alt)
        .mark_bar()
        .encode(
            x=alt.X("Alcaldía:N", title="Alcaldía", sort="-y"),
            y=alt.Y(
                "Porcentaje:Q",
                title="Porcentaje de Traslados",
                axis=alt.Axis(format=".1%"),
            ),
            color=alt.Color("Alcaldía:N", scale=alt.Scale(range=COLORS), legend=None),
            tooltip=[
                "Alcaldía",
                alt.Tooltip("Porcentaje:Q", format=".1%"),
                alt.Tooltip("traslados:Q", title="Total Traslados", format=","),
                alt.Tooltip("total:Q", title="Total Accidentes", format=","),
            ],
        )
        + alt.Chart(res_alt)
        .mark_text(align="center", baseline="bottom", dy=-5, fontWeight="bold", fontSize=8)
        .encode(
            x=alt.X("Alcaldía:N", title="Alcaldía", sort="-y"),
            y=alt.Y("Porcentaje:Q", title="Porcentaje de Traslados"),
            text=alt.Text("total:Q", format=","),
        )
        + alt.Chart(res_alt)
        .mark_text(align="center", baseline="bottom", dy=-20, fontSize=8)
        .encode(
            x=alt.X("Alcaldía:N", title="Alcaldía", sort="-y"),
            y=alt.Y("Porcentaje:Q", title="Porcentaje de Traslados"),
            text=alt.Text("traslados:Q", format=","),
        )
    )
    .properties(
        width=600,
        height=400,
        title="Porcentaje de Accidentes con Traslado por Alcaldía",
    )
    .configure_axis(labelFontSize=12, titleFontSize=14, labelAngle=45)
    .configure_title(fontSize=16)
)

chart
