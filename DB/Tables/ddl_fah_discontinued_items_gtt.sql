/*******************************************************************************************
* Purpose: Logic - Farmacias del Ahorro - GAP 14 Business Rules changed
* Change History:
* Author           Date          Defect#    Change ID   Change Description
*--------------------------------------------------------------------------------------------
* Fabiane Kirsten  01/06/2023    N/A        001         Table FAH_DISCONTINUED_ITEMS_GTT 
*********************************************************************************************/
-- Alter table
alter table FAH_DISCONTINUED_ITEMS_GTT drop column no_movement_period
/
--
alter table FAH_DISCONTINUED_ITEMS_GTT add no_movement_period  number
/

