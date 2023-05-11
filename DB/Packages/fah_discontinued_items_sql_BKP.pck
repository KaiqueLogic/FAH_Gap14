CREATE OR REPLACE PACKAGE FAH.fah_discontinued_items_sql AUTHID CURRENT_USER AS
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

END fah_discontinued_items_sql;
/
CREATE OR REPLACE PACKAGE BODY FAH.fah_discontinued_items_sql IS
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
   -----------------------------------------------------------------------------------------------------
   --Begin 003
     FUNCTION main(O_error_message OUT VARCHAR2,
                   I_thread_no     IN  NUMBER,
                   I_num_threads   IN  NUMBER,
                   I_batch_name    IN  VARCHAR2,
                   I_recovery      IN  NUMBER DEFAULT 0) RETURN BOOLEAN AS
     --FUNCTION main(O_error_message OUT VARCHAR2) RETURN BOOLEAN AS
   --End 003
      L_program CONSTANT VARCHAR2(100) := 'FAH_DISCONTINUED_ITEMS_SQL.MAIN';
      L_new_store_period  NUMBER;
      L_new_assortment    NUMBER;
      L_uda_id_farma      NUMBER;
      L_uda_value_farma   NUMBER;
      L_method_w_farma    NUMBER;
      L_method_w_no_farma NUMBER;
      L_method_s_farma    NUMBER;
      L_method_s_no_farma NUMBER;
      L_value_1           fah_system_parameters.value_1%TYPE;
      L_value_2           fah_system_parameters.value_1%TYPE;
      LP_vdate            period.vdate%TYPE := get_vdate;
      G_user_id VARCHAR2(50) := fah_coresvc_utils.get_user_id();
      C_disc_reason       constant code_detail.code%TYPE := 'NMOS';

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
      CURSOR c_get_item_loc IS
         WITH store_d AS (SELECT store, store_open_date
                            FROM store st
                           WHERE st.store_close_date IS NULL
                             AND EXISTS (SELECT 1
                                           FROM fah_rollout_system_matrix rol
                                          WHERE rol.loc_type = 'W'
                                            AND rol.system = 'ORACLE'
                                            AND rol.loc = st.default_wh))
         SELECT /*+ PARALLEL(6) */ il.item, il.loc
           FROM item_loc il,
                item_loc_soh ils,
                store_d,
                (SELECT item, uda_value
                   FROM uda_item_lov
                  WHERE uda_id = 3) uil,
                (SELECT /*+ PARALLEL(6) */ h.to_loc, d.item, MAX(h.receive_date) last_receive_date
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

      TYPE t_item_loc_tbl IS TABLE OF c_get_item_loc%ROWTYPE;
      L_item_loc_tbl  t_item_loc_tbl;
-- 004 - CMS - End
   BEGIN
      --- NEW_ASSORTMENT_PERIOD
      OPEN c_parameters('NEW_ASSORTMENT_PERIOD');
      FETCH c_parameters
         INTO L_value_1,
              L_value_2;
      IF c_parameters%NOTFOUND THEN
         O_error_message := 'NOT FOUND FAH_parameter NEW_ASSORTMENT_PERIOD';
         CLOSE c_parameters;
         RETURN FALSE;
      ELSE
         CLOSE c_parameters;
         L_new_assortment := to_number(L_value_1);
      END IF;
      --- NEW_STORE_PERIOD
      OPEN c_parameters('NEW_STORE_PERIOD');
      FETCH c_parameters
         INTO L_value_1,
              L_value_2;
      IF c_parameters%NOTFOUND THEN
         O_error_message := 'NOT FOUND FAH_parameter NEW_STORE_PERIOD';
         CLOSE c_parameters;
         RETURN FALSE;
      ELSE
         CLOSE c_parameters;
         L_new_store_period := to_number(L_value_1);
      END IF;
      --- UDA_ID_FARMA_PRODUCT
      OPEN c_parameters('UDA_ID_FARMA_PRODUCT');
      FETCH c_parameters
         INTO L_value_1,
              L_value_2;

      IF c_parameters%NOTFOUND THEN
         O_error_message := 'NOT FOUND FAH_parameter UDA_ID_FARMA_PRODUCT';
         CLOSE c_parameters;
         RETURN FALSE;
      ELSE
         CLOSE c_parameters;
         L_uda_id_farma    := to_number(L_value_1);
         L_uda_value_farma := to_number(L_value_2);
      END IF;

      --- SOURCE_METHOD_S_PERIOD
      OPEN c_parameters('SOURCE_METHOD_S_PERIOD');
      FETCH c_parameters
         INTO L_value_1,
              L_value_2;
      IF c_parameters%NOTFOUND THEN
         O_error_message := 'NOT FOUND FAH_parameter SOURCE_METHOD_S_PERIOD';
         CLOSE c_parameters;
         RETURN FALSE;
      ELSE
         CLOSE c_parameters;
         L_method_s_farma    := to_number(L_value_1);
         L_method_s_no_farma := to_number(L_value_2);
      END IF;

      --- SOURCE_METHOD_W_PERIOD
      OPEN c_parameters('SOURCE_METHOD_W_PERIOD');

      FETCH c_parameters
         INTO L_value_1,
              L_value_2;
      IF c_parameters%NOTFOUND THEN
         O_error_message := 'NOT FOUND FAH_parameter SOURCE_METHOD_W_PERIOD';
         CLOSE c_parameters;
         RETURN FALSE;
      ELSE
         CLOSE c_parameters;
         L_method_w_farma    := to_number(L_value_1);
         L_method_w_no_farma := to_number(L_value_2);
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

       --LOGGER.LOG_INFORMATION(L_program||', processing thread '|| I_thread_no ||', chunk '|| rec.chunk_id ||' (commit_max_ctr = ' || l_commit_max_ctr ||'), DEPS for processing: '||l_tab_ids.count);

       if l_tab_ids.count <= 0 then
          continue;
       end if;
   --End 003

       DELETE FROM fah_discontinued_items_gtt
				   --Begin 003
				    WHERE item IN (select item
				                     from item_master
				                    where dept in (select VALUE(ids)
                                         from table(cast(l_tab_ids as OBJ_NUMERIC_ID_TABLE)) ids));
				   --End 003

-- 004 - CMS - Begin
      OPEN c_get_item_loc;
      LOOP
         FETCH c_get_item_loc
            BULK COLLECT INTO L_item_loc_tbl LIMIT 10000;
         EXIT WHEN L_item_loc_tbl.COUNT = 0;
      
         FORALL i IN 1 .. L_item_loc_tbl.COUNT
            INSERT INTO FAH_DISCONTINUED_ITEMS_GTT
               (ITEM, LOC)
            VALUES
               (L_item_loc_tbl(i).item, L_item_loc_tbl(i).loc);
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

/* --BEGIN 002
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
                         C_disc_reason);

   --Begin 003
       --Commit chunk data
       --LOGGER.LOG_INFORMATION(L_program||' COMMIT thread '|| I_thread_no ||', chunk '|| rec.chunk_id);

       UPDATE restart_bookmark
          SET bookmark_string = rec.chunk_id,
              num_commits = NVL(num_commits,0) + 1
        WHERE restart_name = I_batch_name
          AND thread_val   = I_thread_no;
       COMMIT;

    END LOOP;
   --End 003

      RETURN TRUE;
   EXCEPTION
      WHEN OTHERS THEN
         O_error_message := sql_lib.create_msg('PACKAGE_ERROR', SQLERRM, L_program, to_char(SQLCODE));
         RETURN FALSE;
   END main;
   -----------------------------------------------------------------------------------------------------
END fah_discontinued_items_sql;
/
