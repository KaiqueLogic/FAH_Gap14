CREATE OR REPLACE PACKAGE fah_discontinued_items_sql AUTHID CURRENT_USER AS 
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
   
END fah_discontinued_items_sql;
/
