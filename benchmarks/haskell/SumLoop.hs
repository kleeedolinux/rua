sumTo :: Int -> Int -> Int
sumTo n acc
  | n == 0 = acc
  | otherwise = sumTo (n - 1) (acc + n)

main :: IO ()
main = print (sumTo 200000 0)
