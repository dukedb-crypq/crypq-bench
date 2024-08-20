/*
Find all length-3 cycles where: address A transacted to address B,
address B transacted to address C,
and address C transacted to address A.
The transactions do not need to occur in any particular temporal order.
Sort these cycles by the number of times they come up in the transaction history.
To avoid repetition, we enforce that A is always the smallest among the three addresses.
*/
SELECT t1.from_address, t2.from_address, t3.from_address, COUNT(*)
FROM Transactions t1, Transactions t2, Transactions t3
WHERE t1.to_address = t2.from_address
AND t2.to_address = t3.from_address
AND t3.to_address = t1.from_address
AND t1.from_address < t2.from_address
AND t1.from_address < t3.from_address
GROUP BY t1.from_address, t2.from_address, t3.from_address
ORDER BY 4 DESC;