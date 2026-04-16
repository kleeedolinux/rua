local function sum_to(n)
  local acc = 0
  for i = 1, n do
    acc = acc + i
  end
  return acc
end

print(sum_to(200000))
