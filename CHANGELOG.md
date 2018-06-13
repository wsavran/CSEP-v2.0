### issues:
* [fixed] foreign key inserts not handled properly
* [fixed] inefficient inserts in model script
* [fixed] forecasts present in multiple forecast groups duplicated in database
* [fixed] not parsing forecast meta data correctly
* [fixed] when forecast is present in multiple forecast groups, evaluations are incorrectly expected from each forecast 
group.
* [fixed] missing evaluations having different regex
* [fixed] forecast names not being properly created if inputs present, eg, ETAS_HWMd3 vs ETAS_HW as listed in forecast 
group config
* [\[fixed\]](https://github.com/wsavran/csep_db/pull/11) certain forecasts present but not expected based on forecast group information, eg, ETAS_DROneDayPPE is 
hard-coded into the model implementation in CSEP. exists as ETAS_DROneDayPPE and ETAS_DROneDay on server.
* [fixed] forecasts incorrectly labeled as missing when filename suffix is not correct eg., -fromXML.xml instead of 
.xml
* [fixed] forecasts could be missing from files attribute, but models listed in the models tag produce forecasts with 
unexpected names e.g., K3Md2 forecasts in one-day-models-Md2-V12.10 forecast group. difficult to determine perfect mapping
from model -> without a priori information
* evaluations linking to external forecast groups not being attributed to forecasts introduced in previous forecast 
groups, eg., additional evaluations in one-day-models-V16.4 group
* [fixed] evaluations could contain different filetypes than -fromXML.xml; namely for the TX and WX tests
* [fixed] waiting period applied to evaluations incorrectly
 
### features:
* [done] added tests for model base class
* [done] add forecast group name to schema
* [done] add scheduled tag to status to simplify queries
 

### changes:
* [done] incorporate list of models based on files tag in forecast group init file, and locate missing models.
* [done] added waiting period to dispatcher table
* support for non one-day-models missing, needs to be implemented by parsing forecast group schedule
* [done] add file scanning mechanism to determine any forecasts that are not recognized by forecast group


