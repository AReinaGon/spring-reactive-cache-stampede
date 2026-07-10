CREATE TABLE ticket_availability (
    event_id        VARCHAR(255) PRIMARY KEY,
    event_name      VARCHAR(255) NOT NULL,
    available_seats INT          NOT NULL
);

INSERT INTO ticket_availability (event_id, event_name, available_seats)
VALUES ('black-friday-2026', 'Concierto Black Friday Edition', 1500);
