### issues:
    * [fixed] foreign key inserts not handled properly
	* [fixed] inefficient inserts in model script
    * [fixed] forecasts present in multiple forecast groups duplicated in database
    * [fixed] not parsing forecast meta data correctly
	* [fixed] when forecast is present in multiple forecast groups, evaluations are incorrectly expected from each forecast group.
	* [fixed] missing evaluations having different regex
	* forecast names not being properly created if inputs present
	* certain forecasts not expected based on forecast group information, eg, ETASPPE

### features:
    * [done] added tests for model base class
	* [done] add forecast group name to schema


