USE xkcd;
CREATE TABLE IF NOT EXISTS comics (
	num INT PRIMARY KEY,
	title VARCHAR(255),
	alt TEXT,
	img VARCHAR(255),
	link VARCHAR(255),
	pub_date DATE,
	search_text TEXT,
	FULLTEXT KEY ft_search (search_text)
);
CREATE USER 'xkcd'@'%' IDENTIFIED BY 'xkcd';
GRANT SELECT ON xkcd.comics TO 'xkcd'@'%';
