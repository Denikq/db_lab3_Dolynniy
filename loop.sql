SELECT * FROM Rating

DO $$
DECLARE
    rating_id   rating.rating_id%TYPE;

BEGIN
    rating_id := 0;
    FOR counter IN 1..5
        LOOP
            INSERT INTO rating(rating_id, rating_count, rating_average)
            VALUES (rating_id + counter, counter, counter * 1000);
        END LOOP;
END;
$$
