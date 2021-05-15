// 'https://www.youtube.com/watch?v=Eh_79goBRUk'

// With the following constraints, you enforce uniqueness of each row and create indexes behind the scenes
CREATE CONSTRAINT ON (pl:Playlist) ASSERT pl.id IS UNIQUE;
CREATE CONSTRAINT ON (c:Country) ASSERT c.id IS UNIQUE;
CREATE CONSTRAINT ON (u:User) ASSERT u.name IS UNIQUE;
CREATE CONSTRAINT ON (t:Track) ASSERT t.id IS UNIQUE;
CREATE CONSTRAINT ON (alb:Album) ASSERT alb.id IS UNIQUE;
CREATE CONSTRAINT ON (art:Artist) ASSERT art.id IS UNIQUE;
CREATE CONSTRAINT ON (g:Genre) ASSERT g.name IS UNIQUE;
CREATE CONSTRAINT ON (plg:PLGenre) ASSERT plg.name IS UNIQUE;

// Use artists to initiate graph, along with genres attributed to those artists
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/ebhtra/msds-607/main/spotifyNeo4j/artists.csv' AS line
MERGE (a:Artist {id: line.id, name: line.name, followers: toInteger(line.followers), popularity: toInteger(line.popularity)})
WITH a, line
UNWIND SPLIT(line.genres, ',') AS genre
MERGE (g:Genre {name: genre})
MERGE (a)-[:PLAYS_GENRE]->(g);


// Connect tracks to artists, along with the albums holding the tracks (usually singles)
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/ebhtra/msds-607/main/spotifyNeo4j/trackFrame.csv' AS line
WITH line
MATCH (a:Artist {id: line.artistID})
MERGE (t:Track {id: line.ids, name: line.name, popularity: toInteger(line.popularity), duration: toInteger(line.duration)})
CREATE (t)-[:PERFORMED_BY]->(a);

// Used this to finish the above routine, since it jammed up halfway.  Note the change of URL
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/ebhtra/msds-607/main/spotifyNeo4j/tracktail.csv' AS line
WITH line
MATCH (a:Artist {id: line.artistID})
MERGE (t:Track {id: line.ids, name: line.name, popularity: toInteger(line.popularity), duration: toInteger(line.duration)})
CREATE (t)-[:PERFORMED_BY]->(a);

// Now add song qualities as new properties on existing track nodes
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/ebhtra/msds-607/main/spotifyNeo4j/songfeats.csv' AS line
WITH line
MATCH (t:Track {id: line.id})
SET t.danceability = toFloat(line.danceability), t.energy = toFloat(line.energy), 
	t.loudness = toFloat(line.loudness), t.speechiness = toFloat(line.speechiness),
	t.acousticness = toFloat(line.acousticness), t.instrumentalness = toFloat(line.instrumentalness),
	t.liveness = toFloat(line.liveness), t.valence = toFloat(line.valence), 
	t.key = toInteger(line.key), 
	t.major = toBoolean(line.mode), 
	t.tempo = toFloat(line.tempo), 
	t.time_signature = toInteger(line.time_signature);


// Attach as many tracks as possible to albums, which will introduce dates as well
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/ebhtra/msds-607/main/spotifyNeo4j/trackFrame.csv' AS line
MERGE (a:Album {id: line.albumID})
SET a.name = line.albumName, 
	a.released = date(line.released)
WITH a, line
MERGE (t:Track {id: line.ids})
MERGE (t)-[:ON_ALBUM]->(a);  

// Deal with problematic encoding as before, with tracktail, to finish second half of routine
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM 
'https://raw.githubusercontent.com/ebhtra/msds-607/main/spotifyNeo4j/tracktail.csv' AS line
MERGE (a:Album {id: line.albumID})
SET a.name = line.albumName, 
	a.released = date(line.released)
WITH a, line
MERGE (t:Track {id: line.ids})
MERGE (t)-[:ON_ALBUM]->(a);

// Build Playlist nodes
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/ebhtra/msds-607/main/spotifyNeo4j/PLs.csv' AS line
MERGE (pl:Playlist {id: line.PLid, name: line.name, followers: toInteger(line.followers),
					numTracks: toInteger(line.tracks), collab: toBoolean(line.collab),
					description: COALESCE(line.description, 'none'), public: toBoolean(line.public)});

// Add Users (owners) and relationship to Playlists just created
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/ebhtra/msds-607/main/spotifyNeo4j/PLs.csv' AS line
MATCH (pl:Playlist {id: line.PLid})
MERGE (u:User {name: line.owner})
CREATE (pl)-[:OWNED_BY]->(u);

// Add Country nodes and relations to Playlists
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/ebhtra/msds-607/main/spotifyNeo4j/PLs.csv' AS line
MATCH (pl:Playlist {id: line.PLid})
WITH pl, line
UNWIND SPLIT(line.countries, '|') AS country
MERGE (c:Country {id: country})
MERGE (pl)-[:PLAYS_IN]->(c);

// Add PLGenres from separate csv
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/ebhtra/msds-607/main/spotifyNeo4j/PLgenres.csv' AS line
MATCH (pl:Playlist {id: line.ID})
MERGE (plg:PLGenre {name: line.genre})
MERGE (pl)-[:OF_GENRE]->(plg);

// Add country names from separate csv
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/ebhtra/msds-607/main/spotifyNeo4j/countryCodeNames.csv' AS line
MATCH (c:Country {id: line.codes})
SET c.name = line.names;


// Connect Playlists to Tracks 
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/ebhtra/msds-607/main/spotifyNeo4j/PLs.csv' AS line
MATCH (pl:Playlist {id: line.PLid})
WITH pl, line
UNWIND SPLIT(line.PLtracks, ',') AS trackID
MERGE (t:Track {id: trackID})
MERGE (pl)-[:INCLUDES]->(t);




