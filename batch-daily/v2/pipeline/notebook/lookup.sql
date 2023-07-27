insert overwrite ubi_w.uts_v2_lkp_pages
select
  page_id,
  page_name,
  iframe
from ACCESS_VIEWS.PAGES
where page_id is not null and page_name is not null and iframe is not null
distribute by page_id;

insert overwrite ubi_w.uts_v2_lkp_rotations
select
  rotation_id,
  mpx_chnl_id
from CHOCO_DATA_V.DW_MPX_ROTATIONS
where rotation_id is not null and mpx_chnl_id is not null
distribute by rotation_id;
