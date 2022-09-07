package preserve.service;

import edu.fudan.common.util.Response;
import edu.fudan.common.util.ConsistencyCheckedCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import preserve.entity.*;

import java.util.Date;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.ArrayList;

/**
 * @author fdse
 */
@Service
public class PreserveServiceImpl implements PreserveService {

    @Autowired
    private RestTemplate restTemplate;

    private static final Logger LOGGER = LoggerFactory.getLogger(PreserveServiceImpl.class);

    // an unique id used to identify a single reservation request
    // it is used in distributed cache protocol to store cache keys
    private static long counter = 0;

    /** Cached Functions */
    private final BiFunction<String, HttpHeaders, Response<Contacts>> getContactsById = (contactsId, httpHeaders) -> {
        PreserveServiceImpl.LOGGER.info("[Preserve Other Service][Get Contacts By Id] Getting....");

        HttpEntity requestGetContactsResult = new HttpEntity(httpHeaders);
        ResponseEntity<Response<Contacts>> reGetContactsResult = restTemplate.exchange(
                "http://ts-contacts-service:12347/api/v1/contactservice/contacts/" + contactsId, HttpMethod.GET,
                requestGetContactsResult, new ParameterizedTypeReference<Response<Contacts>>() {
                });

        return reGetContactsResult.getBody();
    };

    private BiFunction<TripAllDetailInfo, HttpHeaders, Response<TripAllDetail>> getTripAllDetailInformation = (gtdi,
            httpHeaders) -> {
        PreserveServiceImpl.LOGGER.info("[Preserve Other Service][Get Trip All Detail Information] Getting....");

        HttpEntity requestGetTripAllDetailResult = new HttpEntity(gtdi, httpHeaders);
        ResponseEntity<Response<TripAllDetail>> reGetTripAllDetailResult = restTemplate.exchange(
                "http://ts-travel-service:12346/api/v1/travelservice/trip_detail", HttpMethod.POST,
                requestGetTripAllDetailResult, new ParameterizedTypeReference<Response<TripAllDetail>>() {
                });

        return reGetTripAllDetailResult.getBody();
    };

    private BiFunction<String, HttpHeaders, String> queryForStationId = (stationName, httpHeaders) -> {
        PreserveServiceImpl.LOGGER.info("[Preserve Other Service][Get Station Name]");

        HttpEntity requestQueryForStationId = new HttpEntity(httpHeaders);
        ResponseEntity<Response<String>> reQueryForStationId = restTemplate.exchange(
                "http://ts-station-service:12345/api/v1/stationservice/stations/id/" + stationName, HttpMethod.GET,
                requestQueryForStationId, new ParameterizedTypeReference<Response<String>>() {
                });

        return reQueryForStationId.getBody().getData();
    };

    private BiFunction<String, HttpHeaders, Response> checkSecurity = (accountId, httpHeaders) -> {
        PreserveServiceImpl.LOGGER.info("[Preserve Other Service][Check Security] Checking....");

        HttpEntity requestCheckResult = new HttpEntity(httpHeaders);
        ResponseEntity<Response> reCheckResult = restTemplate.exchange(
                "http://ts-security-service:11188/api/v1/securityservice/securityConfigs/" + accountId,
                HttpMethod.GET,
                requestCheckResult,
                Response.class);

        return reCheckResult.getBody();
    };

    private BiFunction<Seat, HttpHeaders, Ticket> seatRequestQuery = (seatRequest, httpHeaders) -> {
        HttpEntity requestEntityTicket = new HttpEntity(seatRequest, httpHeaders);
        ResponseEntity<Response<Ticket>> reTicket = restTemplate.exchange(
                "http://ts-seat-service:18898/api/v1/seatservice/seats",
                HttpMethod.POST,
                requestEntityTicket,
                new ParameterizedTypeReference<Response<Ticket>>() {
                });

        return reTicket.getBody().getData();
    };

    private BiFunction<Travel, HttpHeaders, TravelResult> getTicketInfo = (query, headers) -> {
        HttpEntity requestEntity = new HttpEntity(query, headers);
        ResponseEntity<Response<TravelResult>> re = restTemplate.exchange(
                "http://ts-ticketinfo-service:15681/api/v1/ticketinfoservice/ticketinfo", HttpMethod.POST,
                requestEntity, new ParameterizedTypeReference<Response<TravelResult>>() {
                });
        return re.getBody().getData();
    };

    private final ConsistencyCheckedCache<String, HttpHeaders, Response<Contacts>> contactsCache = new ConsistencyCheckedCache<String, HttpHeaders, Response<Contacts>>(
            "contactsCache", 100, true, getContactsById);

    private final ConsistencyCheckedCache<TripAllDetailInfo, HttpHeaders, Response<TripAllDetail>> tripDetailCache = new ConsistencyCheckedCache<TripAllDetailInfo, HttpHeaders, Response<TripAllDetail>>(
            "tripDetailCache", 100, true, getTripAllDetailInformation);

    private final ConsistencyCheckedCache<String, HttpHeaders, String> stationIdCache = new ConsistencyCheckedCache<String, HttpHeaders, String>(
            "stationIdCache", 100, false, queryForStationId);

    private final ConsistencyCheckedCache<String, HttpHeaders, Response> securityCache = new ConsistencyCheckedCache<String, HttpHeaders, Response>(
            "securityCache", 100, true, checkSecurity);

    private final ConsistencyCheckedCache<Travel, HttpHeaders, TravelResult> ticketInfoCache = new ConsistencyCheckedCache<Travel, HttpHeaders, TravelResult>(
            "ticketInfoCache", 100, false, getTicketInfo);

    private final ConsistencyCheckedCache<Seat, HttpHeaders, Ticket> seatRequestCache = new ConsistencyCheckedCache<Seat, HttpHeaders, Ticket>(
            "seatRequestCache", 100, false, seatRequestQuery);

    @Override
    public Response preserve(OrderTicketsInfo oti, HttpHeaders headers) {

        // 0.assign id
        String id = String.valueOf(counter++);
        headers.set("invalidation_id", String.valueOf(id));

        PreserveServiceImpl.LOGGER.info("[Preserve Service] [Step 0] Invalidation ID assigned: {}", id);

        // 1.detect ticket scalper
        PreserveServiceImpl.LOGGER.info("[Preserve Service] [Step 1] Check Security");

        Response result = securityCache.getOrInsert(id, oti.getAccountId(), headers);
        if (result.getStatus() == 0) {
            return new Response<>(0, result.getMsg(), null);
        }
        PreserveServiceImpl.LOGGER.info("[Preserve Service] [Step 1] Check Security Complete");
        // 2.Querying contact information -- modification, mediated by the underlying
        // information micro service
        PreserveServiceImpl.LOGGER.info("[Preserve Service] [Step 2] Find contacts");
        PreserveServiceImpl.LOGGER.info("[Preserve Service] [Step 2] Contacts Id: {}", oti.getContactsId());

        Response<Contacts> gcr = contactsCache.getOrInsert(id, oti.getContactsId(), headers);
        if (gcr.getStatus() == 0) {
            PreserveServiceImpl.LOGGER.info("[Preserve Service][Get Contacts] Fail. {}", gcr.getMsg());
            return new Response<>(0, gcr.getMsg(), null);
        }
        PreserveServiceImpl.LOGGER.info("[Preserve Service][Step 2] Complete");
        // 3.Check the info of train and the number of remaining tickets
        PreserveServiceImpl.LOGGER.info("[Preserve Service] [Step 3] Check tickets num");
        TripAllDetailInfo gtdi = new TripAllDetailInfo();

        gtdi.setFrom(oti.getFrom());
        gtdi.setTo(oti.getTo());

        gtdi.setTravelDate(oti.getDate());
        gtdi.setTripId(oti.getTripId());
        PreserveServiceImpl.LOGGER.info("[Preserve Service] [Step 3] TripId: {}", oti.getTripId());

        Response<TripAllDetail> response = tripDetailCache.getOrInsert(id, gtdi, headers);
        TripAllDetail gtdr = response.getData();
        if (gtdr == null)
            return new Response<>(1, "Get TripAllDetail failed.", "");

        LOGGER.info("TripAllDetail:" + gtdr.toString());
        if (response.getStatus() == 0) {
            PreserveServiceImpl.LOGGER.info("[Preserve Service][Search For Trip Detail Information] {}",
                    response.getMsg());
            return new Response<>(0, response.getMsg(), null);
        } else {
            TripResponse tripResponse = gtdr.getTripResponse();
            if (tripResponse == null)
                return new Response<>(1, "Get TripResponse from triAllDetail failed.", "");

            LOGGER.info("TripResponse:" + tripResponse.toString());

            if (oti.getSeatType() == SeatClass.FIRSTCLASS.getCode()) {
                if (tripResponse.getConfortClass() == 0) {
                    PreserveServiceImpl.LOGGER.info("[Preserve Service][Check seat is enough] ");
                    return new Response<>(0, "Seat Not Enough", null);
                }
            } else {
                if (tripResponse.getEconomyClass() == SeatClass.SECONDCLASS.getCode()
                        && tripResponse.getConfortClass() == 0) {
                    PreserveServiceImpl.LOGGER.info("[Preserve Service][Check seat is enough] ");
                    return new Response<>(0, "Seat Not Enough", null);
                }
            }
        }
        Trip trip = gtdr.getTrip();
        PreserveServiceImpl.LOGGER.info("[Preserve Service] [Step 3] Tickets Enough");
        // 4.send the order request and set the order information
        PreserveServiceImpl.LOGGER.info("[Preserve Service] [Step 4] Do Order");
        Contacts contacts = gcr.getData();
        Order order = new Order();
        UUID orderId = UUID.randomUUID();
        order.setId(orderId);
        order.setTrainNumber(oti.getTripId());
        order.setAccountId(UUID.fromString(oti.getAccountId()));

        String fromStationId = stationIdCache.getOrInsert(id, oti.getFrom(), headers);
        String toStationId = stationIdCache.getOrInsert(id, oti.getTo(), headers);

        order.setFrom(fromStationId);
        order.setTo(toStationId);
        order.setBoughtDate(new Date());
        order.setStatus(OrderStatus.NOTPAID.getCode());
        order.setContactsDocumentNumber(contacts.getDocumentNumber());
        order.setContactsName(contacts.getName());
        order.setDocumentType(contacts.getDocumentType());

        Travel query = new Travel();
        query.setTrip(trip);
        query.setStartingPlace(oti.getFrom());
        query.setEndPlace(oti.getTo());
        query.setDepartureTime(new Date());

        TravelResult resultForTravel = ticketInfoCache.getOrInsert(id, query, headers);

        order.setSeatClass(oti.getSeatType());
        PreserveServiceImpl.LOGGER.info("[Preserve Service][Order] Order Travel Date: {}", oti.getDate().toString());
        order.setTravelDate(oti.getDate());
        order.setTravelTime(gtdr.getTripResponse().getStartingTime());

        // Dispatch the seat
        if (oti.getSeatType() == SeatClass.FIRSTCLASS.getCode()) {
            Ticket ticket = dipatchSeat(id, oti.getDate(),
                    order.getTrainNumber(), fromStationId, toStationId,
                    SeatClass.FIRSTCLASS.getCode(), headers);
            order.setSeatNumber("" + ticket.getSeatNo());
            order.setSeatClass(SeatClass.FIRSTCLASS.getCode());
            order.setPrice(resultForTravel.getPrices().get("confortClass"));
        } else {
            Ticket ticket = dipatchSeat(id, oti.getDate(),
                    order.getTrainNumber(), fromStationId, toStationId,
                    SeatClass.SECONDCLASS.getCode(), headers);
            order.setSeatClass(SeatClass.SECONDCLASS.getCode());
            order.setSeatNumber("" + ticket.getSeatNo());
            order.setPrice(resultForTravel.getPrices().get("economyClass"));
        }

        PreserveServiceImpl.LOGGER.info("[Preserve Service][Order Price] Price is: {}", order.getPrice());

        Response<Order> cor = createOrder(order, headers);
        if (cor.getStatus() == 0) {
            PreserveServiceImpl.LOGGER.info("[Preserve Service][Create Order Fail] Create Order Fail.  Reason: {}",
                    cor.getMsg());
            return new Response<>(0, cor.getMsg(), null);
        }
        PreserveServiceImpl.LOGGER.info("[Preserve Service] [Step 4] Do Order Complete");

        Response returnResponse = new Response<>(1, "Success.", cor.getMsg());


        // order creation succeed, recursively invalidate next level caches
        headers.set("invalidation", "true");
        tripDetailCache.invalidate(id, gtdi, headers, true);

        if (oti.getSeatType() == SeatClass.FIRSTCLASS.getCode()) {
            dipatchSeat(id, oti.getDate(),
                order.getTrainNumber(), fromStationId, toStationId,
                SeatClass.FIRSTCLASS.getCode(), headers);
        } else {
            dipatchSeat(id, oti.getDate(),
                order.getTrainNumber(), fromStationId, toStationId,
                SeatClass.SECONDCLASS.getCode(), headers);
        }


        // 5.Check insurance options
        if (oti.getAssurance() == 0) {
            PreserveServiceImpl.LOGGER.info("[Preserve Service][Step 5] Do not need to buy assurance");
        } else {
            Response addAssuranceResult = addAssuranceForOrder(
                    oti.getAssurance(), cor.getData().getId().toString(), headers);
            if (addAssuranceResult.getStatus() == 1) {
                PreserveServiceImpl.LOGGER.info("[Preserve Service][Step 5] Buy Assurance Success");
            } else {
                PreserveServiceImpl.LOGGER.info("[Preserve Service][Step 5] Buy Assurance Fail.");
                returnResponse.setMsg("Success.But Buy Assurance Fail.");
            }
        }

        // 6.Increase the food order
        if (oti.getFoodType() != 0) {

            FoodOrder foodOrder = new FoodOrder();
            foodOrder.setOrderId(cor.getData().getId());
            foodOrder.setFoodType(oti.getFoodType());
            foodOrder.setFoodName(oti.getFoodName());
            foodOrder.setPrice(oti.getFoodPrice());

            if (oti.getFoodType() == 2) {
                foodOrder.setStationName(oti.getStationName());
                foodOrder.setStoreName(oti.getStoreName());
                PreserveServiceImpl.LOGGER.info("[Food Service]!!!!!!!!!!!!!!!foodstore= {}   {}   {}",
                        foodOrder.getFoodType(), foodOrder.getStationName(), foodOrder.getStoreName());
            }
            Response afor = createFoodOrder(foodOrder, headers);
            if (afor.getStatus() == 1) {
                PreserveServiceImpl.LOGGER.info("[Preserve Service][Step 6] Buy Food Success");
            } else {
                PreserveServiceImpl.LOGGER.info("[Preserve Service][Step 6] Buy Food Fail.");
                returnResponse.setMsg("Success.But Buy Food Fail.");
            }
        } else {
            PreserveServiceImpl.LOGGER.info("[Preserve Service][Step 6] Do not need to buy food");
        }

        // 7.add consign
        if (null != oti.getConsigneeName() && !"".equals(oti.getConsigneeName())) {

            Consign consignRequest = new Consign();
            consignRequest.setOrderId(cor.getData().getId());
            consignRequest.setAccountId(cor.getData().getAccountId());
            consignRequest.setHandleDate(oti.getHandleDate());
            consignRequest.setTargetDate(cor.getData().getTravelDate().toString());
            consignRequest.setFrom(cor.getData().getFrom());
            consignRequest.setTo(cor.getData().getTo());
            consignRequest.setConsignee(oti.getConsigneeName());
            consignRequest.setPhone(oti.getConsigneePhone());
            consignRequest.setWeight(oti.getConsigneeWeight());
            consignRequest.setWithin(oti.isWithin());
            LOGGER.info("CONSIGN INFO : " + consignRequest.toString());
            Response icresult = createConsign(consignRequest, headers);
            if (icresult.getStatus() == 1) {
                PreserveServiceImpl.LOGGER.info("[Preserve Service][Step 7] Consign Success");
            } else {
                PreserveServiceImpl.LOGGER.info("[Preserve Service][Step 7] Consign Fail.");
                returnResponse.setMsg("Consign Fail.");
            }
        } else {
            PreserveServiceImpl.LOGGER.info("[Preserve Service][Step 7] Do not need to consign");
        }

        // 8.send notification
        PreserveServiceImpl.LOGGER.info("[Preserve Service]");

        getAccount(order.getAccountId().toString(), headers);

        // NotifyInfo notifyInfo = new NotifyInfo();
        // notifyInfo.setDate(new Date().toString());

        // notifyInfo.setEmail(getUser.getEmail());
        // notifyInfo.setStartingPlace(order.getFrom());
        // notifyInfo.setEndPlace(order.getTo());
        // notifyInfo.setUsername(getUser.getUserName());
        // notifyInfo.setSeatNumber(order.getSeatNumber());
        // notifyInfo.setOrderNumber(order.getId().toString());
        // notifyInfo.setPrice(order.getPrice());
        // notifyInfo.setSeatClass(SeatClass.getNameByCode(order.getSeatClass()));
        // notifyInfo.setStartingTime(order.getTravelTime().toString());

        // TODO: change to async message serivce
        // sendEmail(notifyInfo, headers);

        return returnResponse;
    }

    public Ticket dipatchSeat(String id, Date date, String tripId, String startStationId, String endStataionId, int seatType,
            HttpHeaders httpHeaders) {
        Seat seatRequest = new Seat();
        seatRequest.setTravelDate(date);
        seatRequest.setTrainNumber(tripId);
        seatRequest.setStartStation(startStationId);
        seatRequest.setDestStation(endStataionId);
        seatRequest.setSeatType(seatType);

        if (httpHeaders.containsKey("invalidation")) {
            PreserveServiceImpl.LOGGER.info("[Preserve Service][dispatchSeat: sending invalidation request]");
            seatRequestCache.invalidate(id, seatRequest, httpHeaders, true);
            return null;
        }

        return seatRequestCache.getOrInsert(id, seatRequest, httpHeaders);
    }

    public boolean sendEmail(NotifyInfo notifyInfo, HttpHeaders httpHeaders) {
        PreserveServiceImpl.LOGGER.info("[Preserve Service][Send Email]");
        HttpEntity requestEntitySendEmail = new HttpEntity(notifyInfo, httpHeaders);
        ResponseEntity<Boolean> reSendEmail = restTemplate.exchange(
                "http://ts-notification-service:17853/api/v1/notifyservice/notification/preserve_success",
                HttpMethod.POST,
                requestEntitySendEmail,
                Boolean.class);

        return reSendEmail.getBody();
    }

    public User getAccount(String accountId, HttpHeaders httpHeaders) {
        PreserveServiceImpl.LOGGER.info("[Cancel Order Service][Get Order By Id]");

        HttpEntity requestEntitySendEmail = new HttpEntity(httpHeaders);
        ResponseEntity<Response<User>> getAccount = restTemplate.exchange(
                "http://ts-user-service:12342/api/v1/userservice/users/id/" + accountId,
                HttpMethod.GET,
                requestEntitySendEmail,
                new ParameterizedTypeReference<Response<User>>() {
                });
        Response<User> result = getAccount.getBody();
        return result.getData();
    }

    private Response addAssuranceForOrder(int assuranceType, String orderId, HttpHeaders httpHeaders) {
        PreserveServiceImpl.LOGGER.info("[Preserve Service][Add Assurance For Order]");
        HttpEntity requestAddAssuranceResult = new HttpEntity(httpHeaders);
        ResponseEntity<Response> reAddAssuranceResult = restTemplate.exchange(
                "http://ts-assurance-service:18888/api/v1/assuranceservice/assurances/" + assuranceType + "/" + orderId,
                HttpMethod.GET,
                requestAddAssuranceResult,
                Response.class);

        return reAddAssuranceResult.getBody();
    }

    private Response<Order> createOrder(Order coi, HttpHeaders httpHeaders) {
        PreserveServiceImpl.LOGGER.info("[Preserve Other Service][Get Contacts By Id] Creating....");

        HttpEntity requestEntityCreateOrderResult = new HttpEntity(coi, httpHeaders);
        ResponseEntity<Response<Order>> reCreateOrderResult = restTemplate.exchange(
                "http://ts-order-service:12031/api/v1/orderservice/order",
                HttpMethod.POST,
                requestEntityCreateOrderResult,
                new ParameterizedTypeReference<Response<Order>>() {
                });

        return reCreateOrderResult.getBody();
    }

    private Response createFoodOrder(FoodOrder afi, HttpHeaders httpHeaders) {
        PreserveServiceImpl.LOGGER.info("[Preserve Service][Add food Order] Creating....");

        HttpEntity requestEntityAddFoodOrderResult = new HttpEntity(afi, httpHeaders);
        ResponseEntity<Response> reAddFoodOrderResult = restTemplate.exchange(
                "http://ts-food-service:18856/api/v1/foodservice/orders",
                HttpMethod.POST,
                requestEntityAddFoodOrderResult,
                Response.class);

        return reAddFoodOrderResult.getBody();
    }

    private Response createConsign(Consign cr, HttpHeaders httpHeaders) {
        PreserveServiceImpl.LOGGER.info("[Preserve Service][Add Condign] Creating....");

        HttpEntity requestEntityResultForTravel = new HttpEntity(cr, httpHeaders);
        ResponseEntity<Response> reResultForTravel = restTemplate.exchange(
                "http://ts-consign-service:16111/api/v1/consignservice/consigns",
                HttpMethod.POST,
                requestEntityResultForTravel,
                Response.class);
        return reResultForTravel.getBody();
    }

}
