package com.example;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import com.example.dbs.NativeRepository;
//import com.moe.dbs.QueryResult;
import com.example.dbs.QueryResult;

//import com.moe.Transfer;
//import com.moe.db.NativeRepository;
//import com.moe.db.QueryResult;

@SpringBootApplication
@EnableScheduling
public class MoeRadTimerservicesApplication {

	
	@Autowired
	NativeRepository nativeRepository;
	
	public static void main(String[] args) {
		SpringApplication.run(MoeRadTimerservicesApplication.class, args);
	}
	
	
	public void createNewRecord() {
		String queryCreateNewRecords="INSERT INTO transfer.transfer_teacher_check\n"
				+ "(teacher_employee_code, teacher_id, teacher_name, kv_code, last_promotion_position_type, teaching_nonteaching,teacher_disability_yn ,\n"
				+ "new_entry,unfrez_flag  )\n"
				+ "select teacher_employee_code, teacher_id, teacher_name, kv_code, last_promotion_position_type, teaching_nonteaching,tp.teacher_disability_yn ,\n"
				+ "'Y','Y' \n"
				+ "from public.teacher_profile tp\n"
				+ "where tp.verify_flag ='SA' \n"
				+ "and tp.teacher_id not in\n"
				+ "(select teacher_id  from transfer.transfer_teacher_check ttc )";
		
		nativeRepository.insertQueries(queryCreateNewRecords);
		System.out.println("Insert in TTC");
	}
	
	Integer count=0;
	 @Scheduled(fixedDelay = 2000000000, initialDelay = 10000)
	  public  void update() throws InterruptedException, ParseException {
		  System.out.println("called timer service");
//		  Integer groundForTrans=0;
//		  if(count==0) {
//		  ++count;
		  createNewRecord();
		  
//          String query8Update="update transfer.transfer_teacher_check set unfrez_flag='P' where unfrez_flag='Y'";
		  String query8Update="update transfer.transfer_teacher_check ttc \n"
			  		+ "set unfrez_flag = 'P'\n"
			  		+ "from public.teacher_profile tp \n"
			  		+ "where  ttc.unfrez_flag ='Y'\n"
			  		+ "and tp.teacher_id = ttc.teacher_id \n"
			  		+ "and tp.verify_flag ='SA'";
          nativeRepository.updateQueries(query8Update);
          
          System.out.println("update ");
          
		  String query2Update="update transfer.transfer_teacher_check ttc\n"
		  		+ "set last_promotion_position_type = tp.last_promotion_position_type ,\n"
		  		+ "     teaching_nonteaching = tp.teaching_nonteaching ,\n"
		  		+ "     teacher_disability_yn = tp.teacher_disability_yn ,\n"
		  		+ "     teacher_age = public.calculate_age_with_reference(tp.teacher_dob::date, '2023-07-01')\n"
		  		+ "from public.teacher_profile tp\n"
		  		+ "where tp.teacher_id  = ttc.teacher_id and ttc.unfrez_flag='P' ";
		  
		  nativeRepository.updateQueries(query2Update);
		  
		  System.out.println("called update1");
		  String query3Update="update transfer.transfer_teacher_check ttc\n"
		  		+ "set personal_status_dfpd = ttp.personal_status_dfpd,\n"
		  		+ "personal_status_mdgd= ttp.personal_status_mdgd,\n"
		  		+ "personal_status_spd = ttp.personal_status_spd,\n"
		  		+ "activestay = ttp.absence_days_one ,\n"
		  		+ "disciplinary_yn = ttp.disciplinary_yn, returnstay=0 \n"
		  		+ "from public.teacher_transfer_profile ttp \n"
		  		+ "where ttp.teacher_id = ttc.teacher_id  and ttc.unfrez_flag='P' ";
		  
		  nativeRepository.updateQueries(query3Update);
		  
		  String query4Update="update transfer.transfer_teacher_check ttc set disciplinary_yn = 2 where disciplinary_yn =1 and ttc.unfrez_flag='P'";
		  nativeRepository.updateQueries(query4Update);
		  String query5Update="update transfer.transfer_teacher_check ttc set disciplinary_yn = 1 where disciplinary_yn =0 and ttc.unfrez_flag='P'";
		  nativeRepository.updateQueries(query5Update);
		  String query6Update="update transfer.transfer_teacher_check ttc set disciplinary_yn = 1 where disciplinary_yn =2 and ttc.unfrez_flag='P'";
		  nativeRepository.updateQueries(query6Update);
		  
		  String query7Update= "update transfer.transfer_teacher_check\n"
		  		+ "set valid_post_for_transfer = 1\n"
		  		+ "where new_entry ='Y'\n"
		  		+ "and last_promotion_position_type  in ('13','10','15','18','11','22','23','21','19','16','14','24','12') and unfrez_flag='P'";
		  
		  nativeRepository.updateQueries(query7Update);
		  
		  System.out.println("called update2");
         
          
//          update1();
          
          
          
          
          
         String deleteQuery1=" select * from transfer.transfer_teacher_check where unfrez_flag='P'";
         
      QueryResult qs=   nativeRepository.executeQueries(deleteQuery1);
      
      if(qs.getRowValue().size()>0) {
         for(int i=0;i<qs.getRowValue().size();i++) {
         String insertQuery="insert into backup.teacher_transfer_profile select * from public.teacher_transfer_profile ttp where ttp.teacher_id = '"+qs.getRowValue().get(i).get("teacher_id")+"'";
         nativeRepository.insertQueries(insertQuery);
         String insertQuery2="insert into  backup.teacher_transfer_details select * from transfer.teacher_transfer_details ttd  where ttd.teacher_id = '"+qs.getRowValue().get(i).get("teacher_id")+"'";
         nativeRepository.insertQueries(insertQuery2);
         String updateQuery=" update public.teacher_transfer_profile ttp \n"
		+ "          set trans_emp_declaraion_date = null , trans_emp_declaration_ip = null , trans_emp_is_declaration = null ,\n"
		+ "              choice_kv1_station_code = null, choice_kv1_station_name = null, \n"
		+ "              choice_kv2_station_code = null,choice_kv2_station_name =null,\n"
		+ "              choice_kv3_station_code =null , choice_kv3_station_name =null,\n"
		+ "              choice_kv4_station_code =null , choice_kv4_station_name = null,\n"
		+ "              choice_kv5_station_code = null, choicekv5_station_name =null \n"
		+ "          where ttp.teacher_id = '"+qs.getRowValue().get(i).get("teacher_id")+"'";
         nativeRepository.updateQueries(updateQuery);


String deleteQuery="delete  from transfer.teacher_transfer_details ttd where ttd.teacher_id ='"+qs.getRowValue().get(i).get("teacher_id")+"'";
System.out.println("Teacher Id--->"+qs.getRowValue().get(i).get("teacher_id"));

nativeRepository.deleteQueries(deleteQuery);

         }
      }
      
      String query10Update="update transfer.transfer_teacher_check set unfrez_flag='N' where unfrez_flag='P'";
      nativeRepository.updateQueries(query10Update);
      
//	  }
	  
	 }
	  
		Integer returnStayCount=0;
	
//		 @Scheduled(fixedDelay = 12000000)
		
		  public  void update1() throws InterruptedException, ParseException {
			  System.out.println("called Schedular");
			String  groundForTrans="0";
//			  ++count;
//			  if(count==1) {
			  String query="select tp.* from public.teacher_profile tp, transfer.transfer_teacher_check ttc where  tp.teacher_id=ttc.teacher_id and unfrez_flag='P'";
			  
			  
			  QueryResult qs=nativeRepository.executeQueries(query);
			  
			  
			  for(int i=0;i<qs.getRowValue().size();i++) {
//			  System.out.println("loop--->"+qs.getRowValue().size());
			  LinkedList<Transfer> transfers = new LinkedList<>();
			  
				String QUERYstation = " select *, DATE_PART('day', work_end_date::timestamp - work_start_date::timestamp) as no_of_days from (\r\n"
						+ "				 	select ksm.station_code , work_start_date , coalesce(work_end_date,'2023-06-30') as work_end_date, twe.ground_for_transfer ,\r\n"
						+ "				 	teacher_id   \r\n"
						+ "				 	from 	public.teacher_work_experience twe , kv.kv_school_master ksm \r\n"
						+ "				 	where teacher_id = '" + qs.getRowValue().get(i).get("teacher_id") + "'"
						+ "				 	and ksm.kv_code = twe.udise_sch_code \r\n"
						+ "				 	order by work_start_date \r\n"
						+ "				 	) aa order by work_start_date desc ";
				
				QueryResult qr = nativeRepository.executeQueries(QUERYstation);
				
				for (int j = 0; j < qr.getRowValue().size(); j++) {
//					System.out.println(qr.getRowValue().get(j).get("work_start_date").toString());

					SimpleDateFormat sObj = new SimpleDateFormat("yyyy-MM-dd");

					Date date1 = sObj
							.parse(String.valueOf(qr.getRowValue().get(j).get("work_start_date").toString()));

					try {
						transfers.add(new Transfer(String.valueOf(qr.getRowValue().get(j).get("station_code")),
								sObj.parse(
										String.valueOf(qr.getRowValue().get(j).get("work_start_date").toString())),
								sObj.parse(String.valueOf(qr.getRowValue().get(j).get("work_end_date").toString())),
								(int) Double
										.parseDouble((String.valueOf(qr.getRowValue().get(j).get("no_of_days"))))));

					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
	//System.out.println(transfers.size());
				
//				List<Transfer> highestThreeRows = getHighestThreeRows(transfers);
//				System.out.println(transfers.get(0).getStartDate());
//				System.out.println(transfers.get(1).getStartDate());
//				System.out.println(transfers.get(2).getStartDate());

//				int returnStay = calculateReturnStay(highestThreeRows);
				System.out.println("transfers--->"+transfers.size());
				int returnStay =0;
				if(qr.getRowValue().size()>2) {
					groundForTrans=	String.valueOf(qr.getRowValue().get(1).get("ground_for_transfer"));
					if(groundForTrans.equalsIgnoreCase("1") || groundForTrans.equalsIgnoreCase("2") ||groundForTrans.equalsIgnoreCase("3") || groundForTrans.equalsIgnoreCase("4") || groundForTrans.equalsIgnoreCase("5") || groundForTrans.equalsIgnoreCase("6") || groundForTrans.equalsIgnoreCase("13") || groundForTrans.equalsIgnoreCase("14") || groundForTrans.equalsIgnoreCase("15") || groundForTrans.equalsIgnoreCase("16") || groundForTrans.equalsIgnoreCase("1") || groundForTrans.equalsIgnoreCase("19")) {
				returnStay = calculateReturnStay(transfers);
					}
				}
				
				
//				int returnStay = calculateReturnStay(transfers);
				
				if(returnStay>0) {
					++returnStayCount;
				System.out.println("returnStay--->"+returnStay+"----returnStayCount--->"+returnStayCount);
		
				}
				
//				System.out.println("found return stay--->"+returnStay);
				
				nativeRepository.updateQueries("update transfer.transfer_teacher_check set returnstay='"+returnStay+"' where teacher_id="+qs.getRowValue().get(i).get("teacher_id"));
				
			  }
			  
			  
			  
			String finalUpdate1= "update transfer.transfer_teacher_check ttc\n"
					  + "set dctenurenoofdays= public.datediff(tp.work_experience_position_type_present_station_start_date::date,'2023-06-30')\n"
					  + "from public.teacher_profile tp \n"
					  + "where ttc.new_entry ='Y'\n"
					  + "and tp.teacher_id = ttc.teacher_id ";
			
			nativeRepository.updateQueries(finalUpdate1);
			
			String finalUpdate2="update transfer.transfer_teacher_check ttc set dctenure_year = floor((dctenurenoofdays+returnstay)/365)";
			nativeRepository.updateQueries(finalUpdate2);
			String finalUpdate3="update transfer.transfer_teacher_check set unfrez_flag='P' where unfrez_flag='Y'";
			
			nativeRepository.updateQueries(finalUpdate3);
			  
//			  }
//			 nativeRepository.updateQueries("update public.teacher_work_experience twe set udise_school_name= ksm.kv_name  from kv.kv_school_master ksm where ksm.kv_code = twe.kv_code  and (udise_school_name is null or udise_school_name='') ");
//			 nativeRepository.updateQueries("update public.teacher_work_experience set kv_code = udise_sch_code where kv_code is null");
//			 nativeRepository.updateQueries("update public.teacher_profile set teacher_dob  = TO_CHAR(teacher_dob::timestamptz, 'YYYY-MM-DD')  where length(teacher_dob) = 24");
//			 nativeRepository.updateQueries("update public.teacher_profile set work_experience_work_start_date_present_kv  = TO_CHAR(work_experience_work_start_date_present_kv::timestamptz, 'YYYY-MM-DD') where length(work_experience_work_start_date_present_kv) = 24");
//			 nativeRepository.updateQueries("update public.teacher_profile set work_experience_position_type_present_station_start_date  = TO_CHAR(work_experience_position_type_present_station_start_date::timestamptz, 'YYYY-MM-DD') where length(work_experience_position_type_present_station_start_date) = 24 ");
//			 nativeRepository.updateQueries("update public.teacher_profile set last_promotion_position_date  = TO_CHAR(last_promotion_position_date::timestamptz, 'YYYY-MM-DD') where length(last_promotion_position_date) = 24");
		  }
		  
		  
		    public static List<Transfer> getHighestThreeRows(List<Transfer> transfers) {
		        return transfers.stream()
		                .sorted(Comparator.comparingInt(Transfer::getRowNumber).reversed())
		                .limit(3)
		                .collect(Collectors.toList());
		    }
		    
		    
			  public  int calculateReturnStay(List<Transfer> transfers) {
			        int totalStay = 0;
			        int key=0;
		       	
			        
			        if (transfers.size() >= 3) {
//			        	System.out.println("54545454----");
			            Transfer firstRow = transfers.get(0);
			            Transfer secondRow = transfers.get(1);
			            Transfer thirdRow = transfers.get(2);
			            
	//   System.out.println(firstRow.getStation()+ "---"+secondRow.getStation()+"-----"+thirdRow.getStation());
			            if (firstRow.getStation().trim().equalsIgnoreCase(thirdRow.getStation().trim())) {
			            	if(!secondRow.getStation().equalsIgnoreCase(firstRow.getStation())) {
			            		
			            		String stationThird=thirdRow.getStation().trim();
			            		
			            	System.out.println("condition match--->"+secondRow.getNoOfDays());	
			            		
			            	if(secondRow.getNoOfDays()<=1095) {
			            		totalStay = thirdRow.getNoOfDays();	
			            		for(int i=3;i<transfers.size();i++) {
			            			if(stationThird.equalsIgnoreCase(transfers.get(i).getStation())) {
			            				totalStay +=transfers.get(i).getNoOfDays();
			            			}else {
			            				break;
			            			}
			            		}
			            	}else {
			            		totalStay =0;
			            	}
			            	
			            	System.out.println(totalStay);
			            	if (key==1)
			            	 return totalStay;
			            	}
			                  
			            } else {
			                System.out.println("Station pattern doesn't match.");
			            }
			        } else {
			            System.out.println("Not enough transfers to check the district pattern.");
			        }
			        
			        	      
			        return totalStay;
			    } 
		    
		
			  
			  public  String calculateReturnStayTenure(String StatinCode, int noOfDays, Date startDate, int typeToCalculate) {
			       
			        String returnString ="0:O:0:0";// 0 Means False , O Other Station , 0 No of Year , 0 No of days
			        try {
			        	int category_id = -1;
			        	
			        	String TenureCalculateQry = "select count(*) as no_of_record, category_id  from uneecops.station_category_mapping scm where scm.station_code ='"+StatinCode+"'   "
			        			 +" and '"+startDate +"' between from_date and to_date "
			        			+ "group by category_id ";
//			        	  Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
//				          Statement stmt = conn.createStatement();
//				          ResultSet rs = stmt.executeQuery(TenureCalculateQry);
				          
			          	QueryResult	rs=nativeRepository.executeQueries(TenureCalculateQry);  
				          if (typeToCalculate == 1) {
			        		
				        	  
				        	  for(int i=0;i<rs.getRowValue().size();i++) {
				        	  
//				        		while(rs.next()) {		        			
				        			category_id =Integer.parseInt(String.valueOf(rs.getRowValue().get(i).get("category_id")));
				        					if(category_id==4) {// For Very Very Hard
							        			double noOf_Year = Double.parseDouble(String.valueOf(noOfDays))/ 364;
							        			if (noOf_Year < 2.0 ) { // Did not Complete 2 Year 
							        				returnString = "1:VH:"+noOf_Year+":"+noOfDays;// 1 = True , VH Very Hard ,  
							        				return returnString;
							        			}
							        			
							        		}else { // For all other category it must be 3
							        			double noOf_Year = Double.parseDouble(String.valueOf(noOfDays))/ 364;
							        			if (noOf_Year < 3.0 ) { 
							        				returnString = "1:O:"+noOf_Year+":"+noOfDays;
							        				return returnString;
							        			}
							        		}
				        					return returnString;		
//				        		}
				        		
				        	  }
				        		
				        		  return returnString;
			        		
			        	}
			        	
			        }catch(Exception e) {
			        	e.printStackTrace();
			        }
			        finally {
			        
			        }
			      
			        return returnString;	
			     
			    }  
	  
	  
	  
	  
	  

}
