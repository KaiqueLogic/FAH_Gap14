CREATE OR REPLACE PACKAGE lgc_fah_discontinued_items_sql  AUTHID CURRENT_USER AS
   /***************************************************************************************/
   /* CREATE DATE - 30-JAN-2020                                                           */
   /* CREATE USER - V.Slusarenko                                                          */
   /* PROJECT     - GAP.RMS.14 Farmacias del Ahorro                                       */
   /* DESCRIPTION - The process discontinued item in a Pharmacy when an item does not sell*/
   /*               in a configurable period of time, in a store                          */
   /***************************************************************************************/
   /***************************************************************************************/
   /* CREATE DATE - 03-JUN-2020                                                           */
   /* CREATE USER - Carlos Costa                                                          */
   /* CHANGE      - 001                                                                   */
   /* DESCRIPTION - Agregacion de index a pedido de FAH CORREO:                           */
   /*                          "Proceso nb.discontinued_items.ksh UAT"                    */
   /***************************************************************************************/
   /***************************************************************************************/
   /* CREATE DATE - 16-JUN-2021                                                           */
   /* CREATE USER - Ruslan Zakusilov                                                      */
   /* CHANGE      - 002                                                                   */
   /* DESCRIPTION - Fix the situation when packitem meets all rules to be discontinued,   */
   /*               but component item does not meet them. In this case both items should */
   /*               be kept active.                                                       */
   /***************************************************************************************/
    /******************************************************************************************************/
   /* CHANGE_ID   - 003                                                                                   */
   /* CREATE DATE - 05-08-2021                                                                            */
   /* CREATE USER - Ruslan Zakusilov                                                                      */
   /* PROJECT     - Farmacias del Ahorro                                                                  */
   /* DESCRIPTION - Added the multithreading processing based on information in RESTART_PROGRAM_STATUS    */
   /*******************************************************************************************************/
   /* CHANGE_ID   - 005                                                                                   */
   /* CREATE DATE - 27-04-2023                                                                            */
   /* CREATE USER - Fabiane Kirsten - fabiane.kirsten@logicinfo                                           */
   /* PROJECT     - Farmacias del Ahorro                                                                  */
   /* DESCRIPTION - GAP 14 Business Rules changed                                                         */
   /*******************************************************************************************************/
 
   ---------------------------------------------------------------------------------------------
   -- Function Name : main
   -- Purpose       : This main external function  called from batch fah_discontinued_items.ksh
   ---------------------------------------------------------------------------------------------

   --Begin 003
   FUNCTION main(O_error_message OUT VARCHAR2,
                 I_thread_no     IN  NUMBER,
                 I_num_threads   IN  NUMBER,
                 I_batch_name    IN  VARCHAR2,
                 I_recovery      IN  NUMBER DEFAULT 0) RETURN BOOLEAN;
   --FUNCTION main(O_error_message OUT VARCHAR2) RETURN BOOLEAN;
   --End 003
   -- 005 - Begin
   FUNCTION purge_data(O_error_message IN OUT VARCHAR2) RETURN BOOLEAN;
   -- 005 - End  
END lgc_fah_discontinued_items_sql;
/
CREATE OR REPLACE PACKAGE BODY lgc_fah_discontinued_items_sql IS
  /******************************************************************************************************/
  /* CHANGE_ID   - 003                                                                                   */
  /* CREATE DATE - 05-08-2021                                                                            */
  /* CREATE USER - Ruslan Zakusilov                                                                      */
  /* PROJECT     - Farmacias del Ahorro                                                                  */
  /* DESCRIPTION - Added the multithreading processing based on information in RESTART_PROGRAM_STATUS    */
  /*******************************************************************************************************/
  /* CHANGE_ID   - 004                                                                                   */
  /* CREATE DATE - 04-05-2022                                                                            */
  /* CREATE USER - CMS Team                                                                              */
  /* PROJECT     - Farmacias del Ahorro                                                                  */
  /* DESCRIPTION - Performance fix on SQL statement, using Bulk Collect Into                             */
  /*******************************************************************************************************/
  /* CHANGE_ID   - 005                                                                                   */
  /* CREATE DATE - 12-04-2023                                                                            */
  /* CREATE USER - Fabiane Kirsten - fabiane.kirsten@logicinfo                                           */
  /* PROJECT     - Farmacias del Ahorro                                                                  */
  /* DESCRIPTION - GAP 14 Business Rules changed                                                         */
  /*******************************************************************************************************/
  -----------------------------------------------------------------------------------------------------
  --Begin 003
  FUNCTION main(O_error_message OUT VARCHAR2,
                I_thread_no     IN  NUMBER,
                I_num_threads   IN  NUMBER,
                I_batch_name    IN  VARCHAR2,
                I_recovery      IN  NUMBER DEFAULT 0) RETURN BOOLEAN AS
  --FUNCTION main(O_error_message OUT VARCHAR2) RETURN BOOLEAN AS
  --End 003
    L_program             CONSTANT VARCHAR2(100) := 'LGC_FAH_DISCONTINUED_ITEMS_SQL.MAIN';
    L_value_1             fah_system_parameters.value_1%TYPE;
    L_value_2             fah_system_parameters.value_1%TYPE;
    --LP_vdate            period.vdate%TYPE := get_vdate;
    G_user_id           VARCHAR2(50) := fah_coresvc_utils.get_user_id();
    C_disc_reason         constant code_detail.code%TYPE := 'NMOS';
    -- 005 - Begin
    L_execute_date        fah_ril_discontin_itens_hist.execute_date%type := sysdate;
    L_incr_pct_new        fah_ril_discontin_itens_hist.incr_pct_old%type := 0;
    L_activate_date_new   fah_ril_discontin_itens_hist.activate_date_old%type := get_vdate;
    L_deactivate_date_new fah_ril_discontin_itens_hist.deactivate_date_old%type := NULL;
    -- 005 - End

    --Begin 003
    l_commit_max_ctr NUMBER;
    l_bookmark_string RESTART_BOOKMARK.BOOKMARK_STRING%TYPE := NULL;

    CURSOR c_check_bookmark IS
           select bookmark_string
             from restart_bookmark
            where restart_name = I_batch_name
              and thread_val   = I_thread_no;

    CURSOR c_get_commit_max_ctr IS
           select commit_max_ctr
             from restart_control
            where program_name = I_batch_name;

    CURSOR get_all_chunks IS
           select chunk_id
             from (select distinct CEIL(DENSE_RANK() OVER(ORDER by deps.dept)/l_commit_max_ctr) chunk_id
                              from deps,
                                   v_restart_dept v
                             where deps.dept = v.driver_value
                                  and v.num_threads = I_num_threads
                                 and v.thread_val = I_thread_no) main
                      where l_bookmark_string IS NULL
               or chunk_id > to_number(l_bookmark_string)
                    order by chunk_id;

    CURSOR get_chunks_ids (lc_chunk_id IN NUMBER) IS
                    select main.dept
                      from(
                                select deps.dept,
                                       CEIL(DENSE_RANK() OVER(ORDER by deps.dept)/l_commit_max_ctr) rank
                                 from deps,
                                      v_restart_dept v
                                 where deps.dept = v.driver_value
                       and v.num_threads = I_num_threads
                       and v.thread_val = I_thread_no) main
                        where main.rank = lc_chunk_id;

    l_tab_ids OBJ_NUMERIC_ID_TABLE;
    --End 003

    CURSOR c_parameters(P_parameter fah_system_parameters.parameter%TYPE) IS
         SELECT value_1,
                value_2
           FROM fah.fah_system_parameters
          WHERE func_area = 'DISCONTINUED_ITEMS'
            AND parameter = P_parameter;

    -- 004 - CMS - Begin
    -- 005 - BEGIN
    /*CURSOR c_get_item_loc IS
         WITH store_d AS (SELECT store, store_open_date
                            FROM store st
                           WHERE st.store_close_date IS NULL
                             AND EXISTS (SELECT 1
                                           FROM fah_rollout_system_matrix rol
                                          WHERE rol.loc_type = 'W'
                                            AND rol.system = 'ORACLE'
                                            AND rol.loc = st.default_wh))
         SELECT \*+ PARALLEL(6) *\ il.item, il.loc
           FROM item_loc il,
                item_loc_soh ils,
                store_d,
                (SELECT item, uda_value
                   FROM uda_item_lov
                  WHERE uda_id = 3) uil,
                (SELECT \*+ PARALLEL(6) *\ h.to_loc, d.item, MAX(h.receive_date) last_receive_date
                   FROM shipment h, shipsku d
                  WHERE h.shipment = d.shipment
                    AND (h.order_no IS NOT NULL
                       OR d.distro_type = 'T' AND h.from_loc_type = 'W')
                 GROUP BY h.to_loc, d.item) ship,
                fah_v_discontinued_items_cfg cfg,
                item_master vim,
                deps d,
                groups g,
                period p
          WHERE il.loc_type = 'S'
            AND il.status = 'A'
            AND il.create_datetime + 90 < p.vdate
            AND il.item = ils.item
            AND il.loc = ils.loc
            AND ils.last_received IS NOT NULL
            AND ils.in_transit_qty = 0
            AND il.loc = store_d.store
            AND store_d.store_open_date + 90 < p.vdate
            AND il.item = ship.item(+)
            AND il.loc = ship.to_loc(+)
            AND NOT EXISTS (SELECT 1
                              FROM repl_item_loc
                             WHERE item = il.item
                               AND loc_type = il.loc_type
                               AND location = il.loc
                               AND (deactivate_date IS NULL OR deactivate_date > p.vdate)
                               AND repl_method IN ('M', 'C')
                               AND DECODE(repl_method, 'M', min_stock, 'C', max_stock) > 1)
            AND il.item = uil.item(+)
            AND cfg.source_method = il.source_method
            AND d.dept IN (SELECT VALUE(ids)
                             FROM TABLE(CAST(l_tab_ids AS OBJ_NUMERIC_ID_TABLE)) ids)
            AND il.item = vim.item
            AND d.dept = vim.dept
            AND d.group_no = g.group_no
            AND cfg.division = g.division
            AND cfg.uda_value = uil.uda_value
            AND il.status_update_date + cfg.no_movement_period < p.vdate
            AND NVL(ils.last_sold, to_date('1900', 'YYYY')) + cfg.no_movement_period < p.vdate
            AND NVL(ship.last_receive_date, to_date('1900', 'YYYY')) + cfg.no_movement_period < p.vdate;
    */
    CURSOR c_get_item_loc IS
         WITH assortment_period  AS (
            SELECT value_1
              FROM fah.fah_system_parameters
             WHERE func_area = 'DISCONTINUED_ITEMS'
               AND parameter = 'NEW_ASSORTMENT_PERIOD'),

         new_store_period AS (
            SELECT value_1
              FROM fah.fah_system_parameters
             WHERE func_area = 'DISCONTINUED_ITEMS'
               AND parameter = 'NEW_STORE_PERIOD'),

         items_dep AS (
            SELECT /*+ PARALLEL(6) */ item,
                   dept,
                   class, 
                   subclass
              FROM item_master im
             WHERE im.dept IN (SELECT VALUE(ids)
                                  FROM TABLE(CAST(l_tab_ids AS OBJ_NUMERIC_ID_TABLE)) ids)
--begin 005 - fabi aki remover
--and im.item = '100249807'--'100246817'
and im.dept IN (255,
469,
26,
149,
264,
265,
250,
274,
224,
319,
55,
256,
459,
259,
332,
320,
443,
464,
257,
418,
283)
--end 005 - fabi aki remover
                                  ),
         it_dep_uda AS (
            SELECT /*+ PARALLEL(6) */ itd.item,
                   cfg.no_movement_period,
                   source_method,
                   'Origen: '||source_method||' - Mundo: '||uda_value_desc||' - Tipo de produto: '||div_name||' - Nr de dias: '||no_movement_period criteria,
                   itd.dept,
                   itd.class,
                   itd.subclass 
              FROM items_dep itd,
                   v_deps vd,
                   fah_v_discontinued_items_cfg cfg,
                   uda_item_lov uil
             WHERE vd.dept=itd.dept
               AND vd.DIVISION=cfg.division
               AND uil.uda_id=3
               AND uil.uda_id=cfg.uda_id
               AND uil.uda_value=cfg.uda_value
               AND uil.item=itd.item),
         loc AS (
            SELECT /*+ PARALLEL(6) */ store
              FROM store,
                   new_store_period
             --	Regla 3 - Solo se tendrá en cuenta las Farmacias con más de 120 días de apertura
             WHERE store_open_date < SYSDATE-new_store_period.value_1
               --Regla 10 - Solo se tendrá en cuenta las Farmacias que operan en el Sistema ORACLE 
               AND store_close_date IS NULL 
               AND EXISTS (SELECT 1         
                             FROM fah_rollout_system_matrix rol
                            WHERE rol.loc_type = 'W'
                              AND rol.system = 'ORACLE'
                              AND rol.loc = default_wh)),
         items_loc AS (
            SELECT /*+ PARALLEL(6) */ ril.item,
                   ril.location,
                   idu.no_movement_period,
                   idu.criteria,
                   ril.incr_pct,
                   ril.activate_date,
                   ril.deactivate_date,
                   ril.min_stock,
                   ril.max_stock,
                   ril.stock_cat,
                   ils.stock_on_hand,
                   il.status,
                   p.vdate,
                   idu.dept,
                   idu.class,
                   idu.subclass
                      
              FROM item_loc il,
                   repl_item_loc ril,
                   it_dep_uda idu,
                   loc,
                   assortment_period, 
                   item_loc_soh ils,
                   period p,
                  (SELECT /*+ PARALLEL(6) */ h.to_loc, d.item, MAX(h.receive_date) last_receive_date
                     FROM shipment h, shipsku d
                    WHERE h.shipment = d.shipment
                      AND (h.order_no IS NOT NULL OR d.distro_type = 'T' AND h.from_loc_type = 'W')
                    GROUP BY h.to_loc, d.item) ship
             WHERE il.item      = ril.item   
               AND il.loc       = ril.location 
               AND il.loc_type  = ril.loc_type 
               AND il.loc       = loc.store
               AND il.item      = idu.item 
               AND il.source_method = idu.source_method
               AND il.loc       = ils.loc
               AND il.item      = ils.item
               AND il.item      = ship.item(+)
               AND il.loc       = ship.to_loc(+)
               AND il.loc_type  = 'S'
               --	Regla 4 - Solo se tomará en cuenta los artículos/ubicaciones que estén Activos, 
               AND il.status    = 'A'
               --Rela 12 - (Nuevo) - El artículo/ubicación tener liga reple con INC PCT a 100 
               AND ril.incr_pct = 100
               --Regla 8 - El artículo debe de estar en el surtido de la Farmacia a más de 90 días
               AND il.create_datetime < p.vdate - assortment_period.value_1
               --Regla 6 - El articulo/ubicación debe de tener por lo menos una recepción en la Farmacia
               AND ils.last_received IS NOT NULL
               --Regla 7 - El artículo no puede tener cantidades en tránsito. 
               AND ils.in_transit_qty = 0
               --Regla 9 - Fecha de última activación sea superior al número de Días sin movimiento
               AND il.status_update_date + idu.no_movement_period < p.vdate 
               --Regla 9 - Fecha de última venta sea superior al número de Días sin movimiento
               AND NVL(ils.last_sold, to_date('1900', 'YYYY')) + idu.no_movement_period < p.vdate
               --Regla 9 - Fecha de última compra (Ordenes de Compra y Transferencias de CeDis a Tienda) sea superior al número de ‘Días sin movimiento’ configurado para el artículo.
               AND NVL(ship.last_receive_date, to_date('1900', 'YYYY')) + idu.no_movement_period < p.vdate
               --Regla 2 - No considerar aritulos com repl_item_loc vigente y 
               --Con método ‘Min/Máx’ y tiene un mínimo definido mayor a uno O Con método ‘Constante’ y tiene un máximo definido mayor a uno
               AND NOT EXISTS (SELECT 1
                                  FROM repl_item_loc aux
                                 WHERE aux.item     = ril.item
                                   AND aux.loc_type = ril.loc_type
                                   AND aux.location = ril.location
                                   AND (aux.deactivate_date IS NULL OR aux.deactivate_date > p.vdate)
                                   AND aux.repl_method IN ('M', 'C') 
                                   AND DECODE(aux.repl_method, 'M', aux.min_stock, 'C', aux.max_stock) > 1) 

              --Regla 5 - Si el artículo es un paquete, se cambia el paquete y su componente.
              /*
              delete from fah_discontinued_items_gtt
               where (item, loc) IN (select gtt.item, gtt.loc
                                       from fah_discontinued_items_gtt gtt, packitem pi
                                      where gtt.item = pi.pack_no
                                        and not exists (select 1
                                               from fah_discontinued_items_gtt b
                                              where b.item = pi.item
                                                and b.loc = gtt.loc)
                                        and exists (select 1
                                               from item_loc il1
                                              where il1.item = pi.item
                                                and il1.loc = gtt.loc
                                                and il1.status = 'A'))

              -------
              delete from fah_discontinued_items_gtt
               where (item, loc) IN (select gtt.item, gtt.loc
                                       from fah_discontinued_items_gtt gtt, packitem pi
                                      where gtt.item = pi.item
                                        and not exists (select 1
                                               from fah_discontinued_items_gtt b
                                              where b.item = pi.pack_no
                                                and b.loc = gtt.loc)
                                        and exists (select 1
                                               from item_loc il1
                                              where il1.item = pi.pack_no
                                                and il1.loc = gtt.loc
                                                and il1.status = 'A'))*/                                   
               ),
            
               --Regla 11 - No haya movimientos de stock (órdenes de compra abiertas,transferencias en tránsito) superior al número de ‘Días sin movimiento’
               open_order_its AS
               (select distinct ol.item
                  from ordhead oh, ordloc ol, period p
                 where oh.order_no = ol.order_no
                   and oh.status in ('S', 'A')
                   and oh.written_date > --to_date('28/04/2023', 'DD/MM/YYYY') - 90)
                                         p.vdate - 90),--items_loc.no_movement_period ,
              items_loc_ord_OPEN AS
               (select ilo.*
                  from items_loc ilo
                 where item not in (select item from open_order_its)),

              tdh_tsf_in as
               (select ilo.*
                  from items_loc_ord_OPEN ilo
                 where not exists
                 (select 1
                          from tran_data_history tdh
                         where tran_code = 30--Transferencias entrantes
                           and tdh.location = ilo.location
                           and tdh.item = ilo.item
                           and tdh.tran_date > ilo.vdate - ilo.no_movement_period)),
              td_tsf_in as
               (select thti.*
                  from tdh_tsf_in thti
                 where not exists (select 1
                          from tran_data td
                         where tran_code = 30--Transferencias entrantes
                           and td.location = thti.location
                           and td.dept = thti.dept
                           and td.class = thti.class
                           and td.subclass = thti.subclass
                           and td.item = thti.item)),

              tdh_tsf_out as
               (select tti.*
                  from td_tsf_in tti
                 where not exists
                 (select 1
                          from tran_data_history tdh
                         where tran_code = 32--Transferencias salientes
                           and tdh.location = tti.location
                           and tdh.item = tti.item
                           and tdh.tran_date > tti.vdate - tti.no_movement_period)),
              td_tsf_out as
               (select thto.*
                  from tdh_tsf_out thto
                 where not exists (select 1
                          from tran_data td
                         where tran_code = 32--Transferencias salientes
                           and td.location = thto.location
                           and td.item = thto.item
                           and td.dept = thto.dept
                           and td.class = thto.class
                           and td.subclass = thto.subclass
                        )),
              tdh_ord as
               (select tto.*
                  from td_tsf_out tto
                 where not exists (select 1
                          from tran_data_history tdh
                         where tran_code = 20--Compras
                           and tdh.item = tto.item
                           and tdh.tran_date > tto.vdate - tto.no_movement_period)),
              td_ord as
               (select tho.*
                  from tdh_ord tho
                 where not exists (select 1
                          from tran_data td
                         where tran_code = 20--Compras
                           and td.item = tho.item
                           and td.dept = tho.dept
                           and td.class = tho.class
                           and td.subclass = tho.subclass
                        )),
              tdh_sales as
               (select tord.*
                  from td_ord tord
                 where not exists(select 1
                          from tran_data_history tdh
                         where tran_code in (1, 2, 3)-- Ventas
                           and tdh.item = tord.item
                           and tdh.location = tord.location
                           and tdh.tran_date > tord.vdate - tord.no_movement_period)),
              td_sales as
               (select ths.*
                  from tdh_sales ths
                 where not exists (select 1
                          from tran_data td
                         where tran_code in (1, 2, 3)-- Ventas
                           and td.item = ths.item
                           and td.location = ths.location
                           and td.dept = ths.dept
                           and td.class = ths.class
                           and td.subclass = ths.subclass)),
                           
              --------------------------------------------        
              on_order AS (
                  select /*+ PARALLEL(6) */ ol.item, ol.location,
                         sum(ol.qty_ordered - nvl(ol.qty_received,0)) on_order
                        ,0 pack_comp_on_order
                    from ordhead oh
                        ,ordloc ol
                        ,td_sales ts
                   where oh.order_no = ol.order_no
                     and oh.status in('S','A')
                     and ol.qty_ordered > nvl(ol.qty_received,0)
                     and ol.location = ts.location
                     and ol.item     = ts.item
                     and oh.written_date <= ts.vdate - ts.no_movement_period
                   group by ol.item, ol.location
                  union all
                  select /*+ PARALLEL(6) */ol.item, ol.location,
                         0 on_order
                        ,sum((ol.qty_ordered - nvl(ol.qty_received,0)) * pq.qty) pack_comp_on_order
                    from ordhead oh
                        ,ordloc ol
                        ,v_packsku_qty pq
                        ,td_sales ts
                   where oh.order_no = ol.order_no
                     and ol.item     = pq.pack_no
                     and oh.status in('S','A')
                     and ol.qty_ordered > nvl(ol.qty_received,0)
                     and ol.location = ts.location
                     and ol.item     = ts.item
                     and oh.written_date <= ts.vdate - ts.no_movement_period
                   group by ol.item, ol.location
               ),
               
             in_transit AS (
                  select /*+ PARALLEL(6) */ td.item, th.to_loc loc
                         ,sum(td.tsf_qty - nvl(td.received_qty,0)) in_transit 
                    from tsfhead th
                        ,tsfdetail td
                        ,td_sales ts 
                   where th.tsf_no = td.tsf_no
                     and th.status in('B','S','A')
                     and tsf_qty > nvl(received_qty,0)
                     and th.to_loc = ts.location
                     and td.item   = ts.item
                     and th.approval_date <= ts.vdate - ts.no_movement_period
                     group by td.item, th.to_loc
                   )             
              ---------------------------------------------------                        
              SELECT /*+ PARALLEL(6) */ 
                     ts.item ,
                     ts.location, 
                     ts.criteria,
                     ts.incr_pct,
                     ts.activate_date,
                     ts.deactivate_date,
                     ts.min_stock,
                     ts.max_stock,
                     ts.stock_cat,
                     ts.stock_on_hand,
                     ts.status,
                     (select (nvl(on_order.on_order,0) + nvl(on_order.pack_comp_on_order,0)) 
                        from on_order 
                       where ts.location  = on_order.location
                         and ts.item = on_order.item) on_order,
                     (select in_transit
                        from in_transit 
                       where ts.location  = in_transit.loc
                         and ts.item = in_transit.item) in_transit
                FROM td_sales ts
               where not exists (select 1
                                   from tsfhead th, tsfdetail td
                                  where th.tsf_no = td.tsf_no
                                    and td.item   = ts.item
                                    and th.to_loc = ts.location
                                    and th.status IN ('B', 'S', 'A')
                                    and th.create_date > ts.vdate - ts.no_movement_period)
              ;            
                      /*
         select \*+ PARALLEL(6) *\ 
                items_loc.item ,
                items_loc.loc, 
                items_loc.criteria,
                items_loc.incr_pct,
                items_loc.activate_date,
                items_loc.deactivate_date,
                items_loc.min_stock,
                items_loc.max_stock,
                items_loc.stock_cat,
                items_loc.stock_on_hand,
                items_loc.status,
                (select (nvl(on_order.on_order,0) + nvl(on_order.pack_comp_on_order,0)) 
                   from on_order 
                  where items_loc.loc  = on_order.location
                    and items_loc.item = on_order.item) on_order,
                (select in_transit
                   from in_transit 
                  where items_loc.loc  = in_transit.loc
                    and items_loc.item = in_transit.item) in_transit
           from items_loc;*/
            
    -- 005 END

    TYPE t_item_loc_tbl IS TABLE OF c_get_item_loc%ROWTYPE;
    L_item_loc_tbl  t_item_loc_tbl;
    
-- 004 - CMS - End
  BEGIN
    --- NEW_ASSORTMENT_PERIOD
    OPEN c_parameters('NEW_ASSORTMENT_PERIOD');
    FETCH c_parameters INTO L_value_1, L_value_2;
    IF c_parameters%NOTFOUND THEN
       O_error_message := 'NOT FOUND FAH_parameter NEW_ASSORTMENT_PERIOD';
       CLOSE c_parameters;
       RETURN FALSE;
    ELSE
       CLOSE c_parameters;
    END IF;
    --- NEW_STORE_PERIOD
    OPEN c_parameters('NEW_STORE_PERIOD');
    FETCH c_parameters INTO L_value_1,L_value_2;
    IF c_parameters%NOTFOUND THEN
       O_error_message := 'NOT FOUND FAH_parameter NEW_STORE_PERIOD';
       CLOSE c_parameters;
       RETURN FALSE;
    ELSE
       CLOSE c_parameters;
    END IF;
    --- UDA_ID_FARMA_PRODUCT
    OPEN c_parameters('UDA_ID_FARMA_PRODUCT');
    FETCH c_parameters INTO L_value_1, L_value_2;
    IF c_parameters%NOTFOUND THEN
       O_error_message := 'NOT FOUND FAH_parameter UDA_ID_FARMA_PRODUCT';
       CLOSE c_parameters;
       RETURN FALSE;
    ELSE
       CLOSE c_parameters;
    END IF;
    --- SOURCE_METHOD_S_PERIOD
    OPEN c_parameters('SOURCE_METHOD_S_PERIOD');
    FETCH c_parameters INTO L_value_1, L_value_2;
    IF c_parameters%NOTFOUND THEN
       O_error_message := 'NOT FOUND FAH_parameter SOURCE_METHOD_S_PERIOD';
       CLOSE c_parameters;
       RETURN FALSE;
    ELSE
       CLOSE c_parameters;
    END IF;
    --- SOURCE_METHOD_W_PERIOD
    OPEN c_parameters('SOURCE_METHOD_W_PERIOD');
    FETCH c_parameters INTO L_value_1, L_value_2;
    IF c_parameters%NOTFOUND THEN
       O_error_message := 'NOT FOUND FAH_parameter SOURCE_METHOD_W_PERIOD';
       CLOSE c_parameters;
       RETURN FALSE;
    ELSE
       CLOSE c_parameters;
    END IF;
    

    --Begin 003
    OPEN c_get_commit_max_ctr;
    FETCH c_get_commit_max_ctr INTO l_commit_max_ctr;
    CLOSE c_get_commit_max_ctr;

    IF l_commit_max_ctr IS NULL THEN
       O_error_message  := SQL_LIB.GET_MESSAGE_TEXT('NO_COMMIT_MAX_CTR',
                                                    I_batch_name,
                                                    NULL,
                                                    NULL);
       RETURN FALSE;
    END IF;
    --LOGGER.LOG_INFORMATION(L_program||', processing thread '|| I_thread_no ||' from ' || I_num_threads);

    IF I_recovery = 1 THEN
       OPEN c_check_bookmark;
       FETCH c_check_bookmark INTO l_bookmark_string;
       CLOSE c_check_bookmark;
    ELSE
       l_bookmark_string := NULL;
    END IF;

    FOR rec IN get_all_chunks
    LOOP
       OPEN get_chunks_ids (rec.chunk_id);
       FETCH get_chunks_ids BULK COLLECT INTO l_tab_ids;
       CLOSE get_chunks_ids;
       if l_tab_ids.count <= 0 then
          continue;
       end if;
    --End 003
         -- 005 - BEGIN
 /*      DELETE FROM fah_discontinued_items_gtt
				   --Begin 003
				    WHERE item IN (select item
				                     from item_master
				                    where dept in (select VALUE(ids)
                                         from table(cast(l_tab_ids as OBJ_NUMERIC_ID_TABLE)) ids));*/
				   --End 003
         -- 005 - End

      -- 004 - CMS - Begin
      OPEN c_get_item_loc;
      LOOP
         FETCH c_get_item_loc
            BULK COLLECT INTO L_item_loc_tbl LIMIT 10000;
         EXIT WHEN L_item_loc_tbl.COUNT = 0;
         -- 005 - BEGIN
         --FORALL i IN 1 .. L_item_loc_tbl.COUNT
         --   INSERT INTO FAH_DISCONTINUED_ITEMS_GTT
         --      (ITEM, LOC)
         --   VALUES
         --      (L_item_loc_tbl(i) .item, L_item_loc_tbl(i).loc);*/

         --Regla 13 - Guardar una tabla histórica de los artículos/ubicaciones cambiadas por el GAP.RMS.14 en la tabla REPL ITEM LOC
         FORALL i IN 1 .. L_item_loc_tbl.COUNT
           insert into FAH.fah_ril_discontin_itens_hist
                 (execute_date,
                  item,
                  location,
                  status,
                  incr_pct_old,
                  incr_pct_new,
                  activate_date_old,
                  activate_date_new,
                  deactivate_date_old,
                  deactivate_date_new,
                  min_stock,
                  max_stock,
                  stock_cat,
                  stock_on_hand,
                  stock_in_transit,
                  stock_on_order,
                  reason_code,
                  criteria,
                  create_id)
           values(L_execute_date,
                  L_item_loc_tbl(i).item,
                  L_item_loc_tbl(i).location,
                  L_item_loc_tbl(i).status,
                  L_item_loc_tbl(i).incr_pct,
                  L_incr_pct_new,
                  L_item_loc_tbl(i).activate_date,
                  L_activate_date_new,
                  L_item_loc_tbl(i).deactivate_date,
                  L_deactivate_date_new,
                  L_item_loc_tbl(i).min_stock,
                  L_item_loc_tbl(i).max_stock,
                  L_item_loc_tbl(i).stock_cat,
                  L_item_loc_tbl(i).stock_on_hand,
                  L_item_loc_tbl(i).in_transit,
                  L_item_loc_tbl(i).on_order,
                  C_disc_reason,
                  L_item_loc_tbl(i).criteria,
                  'DISCONTINUED_ITEMS'--G_user_idG_user_id
                  );

         -- UPDATE ITEM_CFA WITH REASON
         FORALL k IN 1 .. L_item_loc_tbl.COUNT
           merge into fah_v_cfa_il_disc_rsn cfa
             using (select L_item_loc_tbl(k).item item, L_item_loc_tbl(k).location loc
                      from dual
                   ) gtt
                on (cfa.item = gtt.item
                and cfa.loc  = gtt.loc)
            when matched then
                    update set cfa_il_dc_attrib=C_disc_reason
            when not matched then
                    insert(item,loc,group_id,cfa_il_dc_attrib)
                    values(gtt.item,
                           gtt.loc,
                           (SELECT group_id  FROM cfa_attrib_group  WHERE group_view_name='V_FAH_GR_CFA_IL_DISC_RSN' AND rownum = 1),
                           C_disc_reason);

         --UPDATE REPL_ITEM_LOC
         FORALL j IN 1 .. L_item_loc_tbl.COUNT
           UPDATE FAH.LGC_REPL_ITEM_LOC
              SET incr_pct        = L_incr_pct_new ,
                  deactivate_Date = L_deactivate_date_new,
                  activate_date   = L_activate_date_new,
                  last_update_datetime = L_execute_date,
                  last_update_id       = 'DISCONTINUED_ITEMS'--G_user_id
            WHERE item     = L_item_loc_tbl(j).item
              AND location = L_item_loc_tbl(j).location;
         --

         COMMIT;

         -- 005 - BEGIN
      END LOOP;
      CLOSE c_get_item_loc;
      -- 004 - CMS - End

         /*PAL begin*/
        /*Check packs*/
        /*
        insert into fah_discontinued_items_gtt(item, loc)
         select pack.pack_no,
                loc
            from fah_discontinued_items_gtt comp,
                 packitem pack
           WHERE comp.item = pack.item
             and exists (select 1
                               from item_loc il
                             where il.item = pack.pack_no
                               and il.loc = comp.loc
                               and il.status = 'A');
        */

        /*--BEGIN 002
          insert into fah_discontinued_items_gtt(item, loc)
          select comp1.item,
                 loc
              from fah_discontinued_items_gtt pack1,
                   packitem comp1
                 where pack1.item = comp1.pack_no
                   and not exists (select 1
                                      from fah_discontinued_items_gtt b
                                     where b.item = comp1.item
                                       and b.loc = pack1.loc)
                    and exists (select 1
                               from item_loc il1
                             where il1.item = comp1.item
                               and il1.loc = pack1.loc
                               and il1.status = 'A');
          --END 002
          */
          /*PAL end*/

        -- 005 - Begin
/*
dbms_output.put_line('ANTES DELETE GTT: '||to_char(sysdate,'dd/mm/rrrr hh24:mi:ss'));
         delete from fah_discontinued_items_gtt
          where (item,loc) IN
                           (select gtt.item,gtt.loc
                             from fah_discontinued_items_gtt gtt,
                                 packitem pi
                               where gtt.item = pi.pack_no
                                 and not exists (select 1
                                                    from fah_discontinued_items_gtt b
                                                   where b.item = pi.item
                                                     and b.loc = gtt.loc)
                                  and exists (select 1
                                             from item_loc il1
                                           where il1.item = pi.item
                                             and il1.loc = gtt.loc
                                             and il1.status = 'A'))
				   --Begin 003
				        and item IN (select item
				                       from item_master
				                      where dept in (select VALUE(ids)
                                           from table(cast(l_tab_ids as OBJ_NUMERIC_ID_TABLE)) ids));
				   --End 003

         delete from fah_discontinued_items_gtt
         where (item,loc) IN
                        (select gtt.item,gtt.loc
                          from fah_discontinued_items_gtt gtt,
                               packitem pi
                            where gtt.item = pi.item
                              and not exists (select 1
                                                 from fah_discontinued_items_gtt b
                                                where b.item = pi.pack_no
                                                  and b.loc = gtt.loc)
                               and exists (select 1
                                          from item_loc il1
                                        where il1.item = pi.pack_no
                                          and il1.loc = gtt.loc
                                          and il1.status = 'A'))
				   --Begin 003
				        and item IN (select item
				                       from item_master
				                      where dept in (select VALUE(ids)
                                           from table(cast(l_tab_ids as OBJ_NUMERIC_ID_TABLE)) ids));
				   --End 003

         UPDATE item_loc
         SET status = 'C',
             status_update_date = sysdate,
             last_update_datetime = sysdate,
             last_update_id = G_user_id
       WHERE (item, loc) IN (select item,loc from fah_discontinued_items_gtt)
				   --Begin 003
				     AND item IN (SELECT item
				                    FROM item_master
				                   WHERE dept in (SELECT VALUE(ids)
                                        FROM table(cast(l_tab_ids as OBJ_NUMERIC_ID_TABLE)) ids));
				   --End 003

      merge into fah_v_cfa_il_disc_rsn cfa
       --Begin 003
           using (select *
                    from fah_discontinued_items_gtt
                   where item in (select item
				                                from item_master
				                               where dept in (select VALUE(ids)
                                                    from table(cast(l_tab_ids as OBJ_NUMERIC_ID_TABLE)) ids))
                 ) gtt
       --    using fah_discontinued_items_gtt gtt
       --End 003
              on (cfa.item = gtt.item
              and cfa.loc  = gtt.loc)
          when matched then
                  update set cfa_il_dc_attrib=C_disc_reason
          when not matched then
                  insert(item,loc,group_id,cfa_il_dc_attrib)
                  values(gtt.item,
                         gtt.loc,
                         (SELECT group_id  FROM cfa_attrib_group  WHERE group_view_name='V_FAH_GR_CFA_IL_DISC_RSN' AND rownum = 1),
                         C_disc_reason);*/

-- 005 - End

   --Begin 003
       --Commit chunk data
       --LOGGER.LOG_INFORMATION(L_program||' COMMIT thread '|| I_thread_no ||', chunk '|| rec.chunk_id);

       UPDATE restart_bookmark
          SET bookmark_string = rec.chunk_id,
              num_commits = NVL(num_commits,0) + 1
        WHERE restart_name = I_batch_name
          AND thread_val   = I_thread_no;

       COMMIT;

    END LOOP; --get_all_chunks
   --End 003
   
    -- 005 - Begin
    if not purge_data(O_error_message) then
       RETURN FALSE;
    end if;
    -- 005 - End
    
    RETURN TRUE;
  EXCEPTION
    WHEN OTHERS THEN
      O_error_message := sql_lib.create_msg('PACKAGE_ERROR', SQLERRM, L_program, to_char(SQLCODE));
      RETURN FALSE;
  END main;
  -----------------------------------------------------------------------------------------------------
  
  -- 005 - Begin
  FUNCTION purge_data(O_error_message IN OUT VARCHAR2) RETURN BOOLEAN AS
    L_program  CONSTANT VARCHAR2(100) := 'LGC_FAH_DISCONTINUED_ITEMS_SQL.PURGE_DATA';
    L_cmd_del  VARCHAR2(2000);
  BEGIN
    --Regla 14 - Purgar la tabla histórica
    L_cmd_del:= 'delete /*+PARALLEL (hist,4)*/
                   from FAH.fah_ril_discontin_itens_hist hist
                  where get_vdate-trunc(hist.execute_date) > (select value_1
                                                                from fah.fah_system_parameters
                                                               where func_area = ''DISCONTINUED_ITEMS''
                                                                 and parameter = ''PURGE_DATA'')';
    EXECUTE IMMEDIATE l_cmd_del;
    --
    COMMIT;
    --
    RETURN TRUE;
    --
  EXCEPTION
    WHEN OTHERS THEN
      O_error_message := sql_lib.create_msg('PACKAGE_ERROR', SQLERRM, L_program, to_char(SQLCODE));
      RETURN FALSE;
  END purge_data;
  -- 005 - End
  -----------------------------------------------------------------------------------------------------
  
END lgc_fah_discontinued_items_sql ;
/
