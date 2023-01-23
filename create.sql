CREATE TABLE Author
(
	author_id          INT        NOT NULL,
   author_name_1      CHAR(100)  NOT NULL,
   author_name_2      CHAR(100)  NULL,
   author_name_3      CHAR(100)  NULL
);

CREATE TABLE Rating
(
	rating_id          INT    NOT NULL,
   rating_count       INT    NOT NULL,
   rating_average     FLOAT  NOT NULL
);

CREATE TABLE Book
(
	book_id            INT         NOT NULL,
   book_name          CHAR(1000)  NOT NULL,
   book_language      CHAR(50)    NULL,
   book_pages         INT         NULL,
   book_date          DATE        NULL,
   author_id          INT         NOT NULL,
   rating_id          INT         NOT NULL
);

-- -------------------
-- Define primary keys
-- -------------------
ALTER TABLE Author ADD PRIMARY KEY (author_id);
ALTER TABLE Rating ADD PRIMARY KEY (rating_id);
ALTER TABLE Book ADD PRIMARY KEY (book_id);


-- -------------------
-- Define foreign keys
-- -------------------
ALTER TABLE Book ADD CONSTRAINT FK_Book_Author FOREIGN KEY (author_id) REFERENCES Author (author_id);
ALTER TABLE Book ADD CONSTRAINT FK_Book_Rating FOREIGN KEY (rating_id) REFERENCES Rating (rating_id);