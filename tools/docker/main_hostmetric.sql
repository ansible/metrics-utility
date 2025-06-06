INSERT INTO public.main_hostmetric (
    hostname,
    first_automation,
    last_automation,
    last_deleted,
    automated_counter,
    deleted_counter,
    deleted,
    used_in_inventories
) VALUES
('host-01.example.com', '2025-05-01T08:00:00+00', '2025-05-10T14:30:00+00', NULL, 12, 0, false, 3),
('host-02.example.com', '2025-04-28T09:15:00+00', '2025-05-12T16:00:00+00', '2025-05-20T10:00:00+00', 5, 1, true, 1),
('host-03.example.com', '2025-05-03T12:00:00+00', '2025-05-11T13:45:00+00', NULL, 7, 0, false, 2),
('host-04.example.com', '2025-05-02T07:30:00+00', '2025-05-09T15:30:00+00', NULL, 10, 0, false, 5),
('host-05.example.com', '2025-04-30T10:00:00+00', '2025-05-08T11:00:00+00', '2025-05-15T12:00:00+00', 3, 2, true, 0),
('host-06.example.com', '2025-05-01T06:45:00+00', '2025-05-06T13:15:00+00', NULL, 6, 1, true, 1),
('host-07.example.com', '2025-05-04T10:30:00+00', '2025-05-10T12:30:00+00', NULL, 8, 0, false, 4),
('host-08.example.com', '2025-04-29T09:45:00+00', '2025-05-07T14:00:00+00', '2025-05-13T09:30:00+00', 4, 1, true, 2),
('host-09.example.com', '2025-05-05T08:30:00+00', '2025-05-10T16:00:00+00', NULL, 9, 0, false, 3),
('host-10.example.com', '2025-05-03T11:15:00+00', '2025-05-11T10:45:00+00', NULL, 11, 0, false, 2);
;
