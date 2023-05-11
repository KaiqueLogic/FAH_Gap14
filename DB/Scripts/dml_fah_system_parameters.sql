/*******************************************************************************************
* Purpose: Logic - Farmacias del Ahorro - GAP 14 Business Rules changed
* Change History:
* Author           Date          Defect#    Change ID   Change Description
*--------------------------------------------------------------------------------------------
* Fabiane Kirsten  27/04/2023    N/A        001         DISCONTINUED_ITEMS parameters creation
*********************************************************************************************/
delete from fah_system_parameters 
 where func_area = 'DISCONTINUED_ITEMS' 
   and parameter in('PURGE_DATA');
--
insert into fah_system_parameters (func_area, parameter, description, value_1, value_2, last_update_id, last_update_date)
values ('DISCONTINUED_ITEMS', 'PURGE_DATA', 'Days for fah_ril_discontin_itens_hist purge', 90, null, 'FAH', sysdate);  
--
commit;

