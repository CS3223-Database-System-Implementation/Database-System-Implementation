SELECT CUSTOMER.gender,MAX(BILL.amount)
FROM CUSTOMER,CART,CARTDETAILS,BILL
WHERE CUSTOMER.cid=CART.cid,CART.cartid=CARTDETAILS.cartid,CARTDETAILS.iid=BILL.iid,BILL.amount<"1000"
GROUPBY CUSTOMER.gender

