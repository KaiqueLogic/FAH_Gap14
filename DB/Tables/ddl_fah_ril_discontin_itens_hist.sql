/*******************************************************************************************
* Purpose: Logic - Farmacias del Ahorro - GAP 14 Business Rules changed
* Change History:
* Author           Date          Defect#    Change ID   Change Description
*--------------------------------------------------------------------------------------------
* Fabiane Kirsten  24/04/2023    N/A        001         Table FAH_RIL_DISCONTIN_ITENS_HIST Creation
*********************************************************************************************/
begin
  execute immediate 'drop table FAH_RIL_DISCONTIN_ITENS_HIST';
exception
  when others then
    dbms_output.put_line(sqlerrm);
end;
/
-- Create table
create table FAH_RIL_DISCONTIN_ITENS_HIST
(
execute_date              DATE not null,  
item                      VARCHAR2(25) not null,
location                  NUMBER(10) not null,
status                    VARCHAR2(1),   
incr_pct_old              NUMBER(12,4),  
incr_pct_new              NUMBER(12,4),  
activate_date_old         DATE,          
activate_date_new         DATE,          
deactivate_date_old       DATE,          
deactivate_date_new       DATE,          
min_stock                  NUMBER(12,4),  
max_stock                 NUMBER(12,4),  
stock_on_hand             NUMBER(12,4),  
stock_in_transit          NUMBER(12,4),  
stock_on_order            NUMBER(12,4),  
stock_cat                 VARCHAR2(6),   
reason_code               VARCHAR2(6),   
criteria                  VARCHAR2(200), 
create_id                 VARCHAR2(30) not null
)
tablespace FAH_DATA
/

-- Add comments to the table
comment on table FAH_RIL_DISCONTIN_ITENS_HIST is 'Stores the history of changes made by the lgc_fah_discontinued_items_sql process';
-- Add comments to the columns 
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.EXECUTE_DATE        is 'Execution date';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.ITEM                is 'Item from repl_item_loc';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.LOCATION            is 'Location from repl_item_loc';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.STATUS              is 'Status from item_loc';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.INCR_PCT_OLD        is 'Old Incr_pct from repl_item_loc';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.INCR_PCT_NEW        is 'New Incr_pct from repl_item_loc';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.ACTIVATE_DATE_OLD   is 'Old activate_date from repl_item_loc';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.ACTIVATE_DATE_NEW   is 'New activate_date from repl_item_loc';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.DEACTIVATE_DATE_OLD is 'Old deactivate_date from repl_item_loc';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.DEACTIVATE_DATE_NEW is 'New deactivate_date from repl_item_loc';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.MIN_STOCK           is 'Min_stock from repl_item_loc';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.MAX_STOCK           is 'Max_stock from repl_item_loc';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.STOCK_ON_HAND       is 'Stock_on_hand from item_loc_soh';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.STOCK_IN_TRANSIT    is 'In_transit_qtd from item_loc_soh';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.STOCK_ON_ORDER      is 'Stock on order from orders';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.STOCK_CAT           is 'Stock_cat from repl_item_loc';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.REASON_CODE         is 'Reason code NMOS';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.CRITERIA            is 'Criteria to Dias sin Movimiento (Origen - Mundo - Tipo de Producto - Numero de Dias)';
comment on column FAH_RIL_DISCONTIN_ITENS_HIST.CREATE_ID           is 'User ID';

-- Create/Recreate primary, unique and foreign key constraints 
alter table FAH_RIL_DISCONTIN_ITENS_HIST  add constraint FAH_RIL_DIH_PK primary key (execute_date, item, location)
/

 



