package com.services.easy2move.service;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.persistence.NonUniqueResultException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.Gson;
import com.services.easy2move.customExceptionMethods.UserCustomException;
import com.services.easy2move.json.Easy2moveuserJson;
import com.services.easy2move.json.Message;
import com.services.easy2move.json.RzpContactResponseJson;
import com.services.easy2move.json.UserBankAccountJson;
import com.services.easy2move.json.UserContactJson;
import com.services.easy2move.model.Easy2moveUsermodel;
import com.services.easy2move.model.UserBackupModel;
import com.services.easy2move.model.UserBankDetailsModel;
import com.services.easy2move.model.UserModel;
import com.services.easy2move.repository.Easy2moveUserRepository;
import com.services.easy2move.repository.UserBackupRepository;
import com.services.easy2move.repository.UserBankDetailsRepository;
import com.services.easy2move.repository.UserRepository;

@Service
public class Easy2moveUserService {
	static final Logger logger = LoggerFactory.getLogger(Easy2moveUserService.class);

	@Autowired
	private Easy2moveUserRepository userrpRepository;

	@Autowired
	private UserBankDetailsRepository userbankdetailsrepository;

	@Autowired
	private UserRepository userRepository;

	@Autowired
	private PasswordEncoder passwordEncoder;

	@Autowired
	private UserBackupRepository userBackupRepository;

	@Autowired
	private Easy2moveUserRepository easy2moveUserRepository;

	@Autowired
	private JmsTemplate jmsTemplate;

	@Value("${razorpay.apikey}")
	private String username;

	@Value("${razorpay.apiSecret}")
	private String password;

	@Value("${razorpay.createcontactAPI}")
	private String contactAPI;

	@Autowired
	RestTemplate restTemplate;
	
	@Autowired
	private KafkaTemplate<String, Easy2moveUsermodel> kafkaTemplate;


	public Easy2moveuserJson adduser(Easy2moveuserJson userjson) {
		Optional<Easy2moveUsermodel> userdata = userrpRepository.findbyMobNo(userjson.getMobile_number());
		if (userdata.isPresent()) {
			throw new UserCustomException("user already registerd please login", 400);
		} else {
			if (userjson.getId() == 0) {
				Easy2moveUsermodel usermodel = new Easy2moveUsermodel();
				usermodel.setName(userjson.getName());

				try {
					usermodel.setUsername(userjson.getUsername());
				} catch (DataIntegrityViolationException ex) {
					throw new DataIntegrityViolationException(ex.getMessage());
				}
				usermodel.setUser_password(passwordEncoder.encode(userjson.getUser_password()));
				usermodel.setCity(userjson.getCity());
				usermodel.setEmail_id(userjson.getEmail_id());
				usermodel.setMobile_number(userjson.getMobile_number());
				usermodel.setIs_active(true);
				usermodel.setCreated_on(new Date());
				usermodel.setUpdated_on(new Date());
				userrpRepository.save(usermodel);
			} else {
				// update user details here
			}
		}

		return userjson;
	}

	public Easy2moveuserJson getuserdetails(String mobileNo) {
		Easy2moveuserJson userjson = new Easy2moveuserJson();
		try {
			Optional<Easy2moveUsermodel> userdata = userrpRepository.findbyMobNo(mobileNo);

			List<Easy2moveUsermodel> list = userdata.stream().filter(u -> u.getMobile_number() != null).
			// map(userdata ,userjson).
					collect(Collectors.toList());

			System.out.println(list);

			if (userdata.isPresent() && userdata.get() != null) {

				userjson.setCity(userdata.get().getCity());
				userjson.setUsername(userdata.get().getUsername());
			} else {
				throw new NoSuchElementException();
			}

		} catch (NonUniqueResultException e) {
			System.out.println("data result set is more than 1 we expect only 1 result");
		}

		return userjson;
	}

//	List<Customer> customersWithMoreThan100Points = customers
//			  .stream()
//			  .filter(c -> c.getPoints() > 100)
//			  .collect(Collectors.toList());

	@Async
	public CompletableFuture<List<UserModel>> saveUsers(MultipartFile file) throws Exception {
		long start = System.currentTimeMillis();
		List<UserModel> users = null;
		if (file.getSize() != 0) {
			users = parseCSVFile(file);
			logger.info("saving list of users of size {}", users.size(), "" + Thread.currentThread().getName());
			users = userRepository.saveAll(users);
			long end = System.currentTimeMillis();
			logger.info("Total time {}", (end - start));
		} else {
			throw new FileNotFoundException();
		}

		return CompletableFuture.completedFuture(users);
	}

	private List<UserModel> parseCSVFile(final MultipartFile file) throws Exception {
		final List<UserModel> users = new ArrayList<>();
		try {

			try (final BufferedReader br = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
				String line;
				while ((line = br.readLine()) != null) {
					final String[] data = line.split(",");
					final UserModel user = new UserModel();
					user.setName(data[0]);
					user.setCity(data[1]);
					user.setEmail(data[2]);
					user.setGender(data[3]);
					users.add(user);
				}
				return users;
			}

		} catch (final IOException e) {
			logger.error("Failed to parse CSV file {}", e);
			throw new Exception("Failed to parse CSV file {}", e);
		}
	}

	public List<UserModel> getuserslist() {

		List<UserModel> myList = new ArrayList<UserModel>();

		Pageable pageRequest = PageRequest.of(0, 200);
		Page<UserModel> userlist = userRepository.findAll(pageRequest);

		while (!userlist.isEmpty()) {
			pageRequest = pageRequest.next();

			// DO SOMETHING WITH ENTITIES
			userlist.stream().

					filter(u -> u.getGender().equalsIgnoreCase("Male")).forEach(entity -> myList.add(entity));
			userlist = userRepository.findAll(pageRequest);

		}

		return myList;
	}

	public String getnamebyusingMQ(String name) {
		// Gson gson = new Gson();

		jmsTemplate.convertAndSend("myname", name);
		return name;
	}

	public void getNamebyMQ(String name) {

		System.out.println(name);
	}

	public Easy2moveuserJson AdduserByMQ(Easy2moveuserJson userjson) {
		Optional<Easy2moveUsermodel> userdata = userrpRepository.findbyMobNo(userjson.getMobile_number());
		if (userdata.isPresent()) {
			throw new UserCustomException("user already registerd please login", 400);
		} else {
			try {
				Easy2moveUsermodel usermod = new Easy2moveUsermodel();
				usermod.setUsername(userjson.getUsername());
				if (usermod.getUsername() != null) {
					Gson gson = new Gson();
					String userdetails = gson.toJson(userjson);
					jmsTemplate.convertAndSend("userDetails", userdetails);
					logger.info("Active MQ listend userdetails" + userdetails);
				}
			} catch (DataIntegrityViolationException ex) {
				throw new DataIntegrityViolationException(ex.getMessage());
			}

		}
		return userjson;
	}

	public void userdetailsgettingfromMQ(Easy2moveuserJson userjson) {

		Easy2moveUsermodel usermodel = new Easy2moveUsermodel();
		usermodel.setUsername(userjson.getUsername());
		usermodel.setName(userjson.getName());
		usermodel.setUser_password(passwordEncoder.encode(userjson.getUser_password()));
		usermodel.setCity(userjson.getCity());
		usermodel.setEmail_id(userjson.getEmail_id());
		usermodel.setMobile_number(userjson.getMobile_number());
		usermodel.setIs_active(true);
		usermodel.setCreated_on(new Date());
		usermodel.setUpdated_on(new Date());
		userrpRepository.save(usermodel);

	}

	public void userScheduleMethod(String name, String userName, String mobile, String email, String password,
			String city) {
		Optional<UserBackupModel> userbkp = userBackupRepository.findbyMobNo(mobile);
		if (userbkp.isEmpty()) {
			UserBackupModel userbModel = new UserBackupModel();
			userbModel.setName(name);
			userbModel.setUsername(userName);
			userbModel.setMobile_number(mobile);
			userbModel.setCity(city);
			userbModel.setEmail_id(email);
			userbModel.setUser_password(password);
			userbModel.setCreated_on(new Date());
			userbModel.setUpdated_on(new Date());
			userbModel.setIs_active(true);
			userbModel = userBackupRepository.save(userbModel);
			System.out.println(userbModel);
		}
	}

	public Message addContactid(UserContactJson userContactJson) {
		Message msg = new Message();
		Optional<Easy2moveUsermodel> userdetails = easy2moveUserRepository.findbyMobNo(userContactJson.getContact());
		if (userdetails.isPresent()) {
			Optional<UserBankDetailsModel> getbankdetails = userbankdetailsrepository.findbyMobile(userContactJson.getContact());
			if (getbankdetails.isPresent()) {
				msg.setMessage("contact details already available");
				msg.setStatuscode(400);
			} else {

				ResponseEntity<RzpContactResponseJson> responseEntity;
				UserBankDetailsModel bankdetails = new UserBankDetailsModel();
				HttpHeaders headers = new HttpHeaders();
				headers.setContentType(MediaType.APPLICATION_JSON);
				headers.setBasicAuth(username, password);
				HttpEntity<UserContactJson> entity = new HttpEntity<UserContactJson>(userContactJson, headers);
				responseEntity = restTemplate.exchange(contactAPI, HttpMethod.POST, entity,
						RzpContactResponseJson.class);
				if (responseEntity.getBody() != null) {
					bankdetails.setUserid(userdetails.get());
					bankdetails.setRzpcontactId(responseEntity.getBody().getId());
					bankdetails.setMobile_number(responseEntity.getBody().getContact());
					bankdetails.setEmail_id(responseEntity.getBody().getEmail());
					bankdetails.setIs_active(true);
					bankdetails.setCreated_on(new Date());
					bankdetails.setUpdated_on(new Date());
					userbankdetailsrepository.save(bankdetails);
					msg.setMessage("contact details added succesfully in razorpay");
					msg.setStatuscode(200);
				}
			}
		} else {
			throw new UsernameNotFoundException("userdetails not found");
		}

		return msg;
	}

	public Message addbankId(UserBankAccountJson userBankAccountJson) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void sendMessageToTopic(String message) {
		System.out.println(message);
	}

	public Easy2moveuserJson getuserdetailsbyMob(String mobileNo) {
		try {
			Optional<Easy2moveUsermodel> userdata = userrpRepository.findbyMobNo(mobileNo);
			Easy2moveUsermodel model = userdata.get();
			kafkaTemplate.send("demo-topic", model);
			
		}catch (Exception e) {
			// TODO: handle exception
		}
		return null;
	}

	public void senduserdata(Easy2moveUsermodel messageReceived) {
		Easy2moveuserJson userjson = new Easy2moveuserJson();
		userjson.setCity(messageReceived.getCity());
		System.out.println(userjson);
	}
}
