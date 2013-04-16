Yes the roads in USA are straighter than Europe.

Motivation

Methodology

First I download the OSM planet file, which was about 19GiB

Using osmosis, I was able to reduce that file to only have ways have ``highway`` tag, and removed all the tags from ways that weren't highway. This brought the file down to about 4.7GiB. I can't remember exactly the command I used because it's gone from my bash history, but it was something like this:

    osmosis # FIXME fill in

It was imported into a PostGIS database with this osm2pgsql style:

    node,way   highway      text         linear

This part took the longest. It took about 5 days on my laptop to import into postgresql. You'll also need a lot of disk space.

Flaws with approach

Obvious one: OpenStreetMap is not complete yet, and is missing many roads that exist in the real world.

In order to speed up the SQL query, it counts the enterity of a way (incl it's ratio) in the bbox's results if any part of the way is in the bbox. This means a very long way that passed through 2 (or more) bbo

It treats each single way element as a different road. If a way is split (into 2 ways) these will then count separately and one road that's very bendy, will appear as several less bendy roads. (e.g.: this way http://www.openstreetmap.org/?way=72436076 ). As an extreme example of this '2 element' ways, which will obviously count as perfectly straight roads. Ways in OSM are often split, not because they are separate roads, but due to how OSM stores data. To solve this, one needs a way to merge connecting ways together.

One approach to merge ways together to get 'real roads', is to merge ways whose endpoints touch if they have the same 'ref'. The ref(erence) of a road (e.g. "N1"), will often show you what is the "natural course of the road", as decided by local planners. Refs, unlike names, often have bery little symbolic or sentimental connection, so local road planners are able to assign them much more freely, giving more accurate results. Here in Ireland, it's not uncommon for one long 'real road' to change names at arbitrary junctions.

Further work

