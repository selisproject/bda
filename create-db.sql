CREATE TABLE kpi_catalogue
(
    id		serial primary key,
    expression	VARCHAR(80) not null,
    timestamp	timestamp default current_timestamp
);
