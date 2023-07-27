create table ubi_w.uts_v2_lkp_rotations (
  rotation_id long,
  mpx_chnl_id int
)
USING avro
LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/uts_v2_lkp_rotations';

create table ubi_w.uts_v2_lkp_pages (
  page_id int,
  page_name string,
  iframe int
)
USING avro
LOCATION 'viewfs://apollo-rno/sys/edw/working/ubi/ubi_w/soj/uts_v2_lkp_pages';
