# Changelog for Version ``1.0.x``

## ``1.0.0`` (2025-12-25)

### Additions
- New data sources replacing ``MRMS_RotationTrack_30min``

``MRMS_MergedAzShear_0-2kmAGL``

``MRMS_MergedAzShear_3-6kmAGL``

### Changes

### API
- API now follows RESTful API design
- Data fetching is now done via GET requests at 

``
/features/fetch/resources?type=[list | cell]
``

which returns a list of available timestamps for ``type=list`` and a list of cell IDs for ``type=cell``

- Data downloading is now done via GET requests at 

``
/features/download/resources?type=[list | cell]&[timestamp | id]=VALUE
``

where ``VALUE`` is the timestamp for ``type=list`` in ``YYYYMMDD-HHMM00`` format and the cell ID for ``type=cell``