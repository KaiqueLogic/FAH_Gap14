/*******************************************************************************************
* Purpose: Logic - Farmacias del Ahorro - GAP 14 Business Rules changed
* Change History:
* Author           Date          Defect#    Change ID   Change Description
*--------------------------------------------------------------------------------------------
* Fabiane Kirsten  24/04/2023    N/A        001         Table FAH_RIL_DISCONTIN_ITEMS_HIST Creation
*********************************************************************************************/
begin
  execute immediate 'drop table FAH_DISCONTINUED_ITEMS_GTT';
exception
  when others then
    dbms_output.put_line(sqlerrm);
end;
/

-- Create table
create global temporary table FAH_DISCONTINUED_ITEMS_GTT
(
  item VARCHAR2(25) not null,
  loc  NUMBER(10) not null,
  no_movement_period  number
)
on commit delete rows;
-- Grant/Revoke object privileges 
grant select on FAH_DISCONTINUED_ITEMS_GTT to "ALFONSO.BUENROSTRO";
grant select on FAH_DISCONTINUED_ITEMS_GTT to ARAMIREZ;
grant select on FAH_DISCONTINUED_ITEMS_GTT to "ITZA.LOPEZ";
grant select on FAH_DISCONTINUED_ITEMS_GTT to "MARTIN.GARCIA";
grant select, insert, update, delete on FAH_DISCONTINUED_ITEMS_GTT to RMS;
grant select on FAH_DISCONTINUED_ITEMS_GTT to RMS_CONSULTA;
grant select on FAH_DISCONTINUED_ITEMS_GTT to "VALTRE_BERNARDO.SOTA";
