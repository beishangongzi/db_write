
createTable = """CREATE TABLE  IF NOT EXISTS {}
(
    id bigserial NOT NULL,                          --自增加id
--     location geometry,
    geometry Geometry(MULTIPOLYGON, 4326),
    BID char(7),                                  --其它信息
    typeLab char(5),
    year smallint null,
    level smallint,
    bArea decimal,
    tArea decimal,
    subtype char(10),
    province varchar(10),
    city varchar(20),
    county varchar(20),
    adcode varchar(6),
    createtime timestamp with time zone,         --时间戳
    updatetime timestamp with time zone,
    PRIMARY KEY (id)
);
"""